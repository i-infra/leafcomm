import asyncio
import atexit
import functools
import inspect
import itertools
import json
import logging
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import time
import traceback
from typing import Any, Callable

try:
    import faulthandler
except ImportError:
    logging.warning("no faulthandler, traceback functionality broken")
    faulthandler = None

# try to import pyroute2 for netns functionality
try:
    import pyroute2
except ImportError:
    logging.warning("no pyroute2, namespace functionality broken")
    pyroute2 = None

try:
    import aioredis
except:
    logging.warning("no aioredis, redis functionality broken")
    aioredis = None

# try to import re2 instead of default re
try:
    import re2

    re = re2
except ImportError:
    import re

    logging.warning("using default regexp package, not libre2")

# try to import uvloop to accelerate asyncio functionality
try:
    import uvloop
except ImportError:
    uvloop = None

DEBUG = bool(os.getenv("DEBUG") or "--debug" in sys.argv)
CURRENT_FILE = globals().get("__file__") or ""

# try to import smhasher for a fast hash, fall back to md5
try:
    import smhasher

    def hasher(string_key):
        return abs(smhasher.murmur3_x64_64(string_key))


except:
    import hashlib

    def hasher(string_key):
        return int.from_bytes(hashlib.md5(string_key.encode()).digest(), "little")


def rerle(xs):
    return [
        (sum([i[0] for i in x[1]]), x[0]) for x in itertools.groupby(xs, lambda x: x[1])
    ]


def rle(xs):
    return [(len(list(gp)), x) for x, gp in itertools.groupby(xs)]


def rld(xs):
    return itertools.chain.from_iterable((itertools.repeat(x, n) for n, x in xs))


def _flatten(l):
    return list(itertools.chain(*[[x] if type(x) not in [list] else x for x in l]))


def debinary(ba):
    return sum([x * 2 ** i for i, x in enumerate(reversed(ba))])


# get handful of relevant parameters
def diag():
    return {
        "pyvers": sys.hexversion,
        "exec": sys.executable,
        "pid": os.getpid(),
        "cwd": os.getcwd(),
        "prefix": sys.exec_prefix,
        "sys.path": sys.path,
    }


# python introspection
get_function_name = lambda depth=0: sys._getframe(depth + 1).f_code.co_name
# match IP addresses
ip_regex = re.compile("(?:[0-9]{1,3}\.){3}[0-9]{1,3}")
# path to currently used linker
linker = [
    x for x in open("/proc/%s/maps" % os.getpid()).read().split("\n") if "ld" in x
][0].split()[-1]

# returns True if maybe_false(x) evaluates to False for any element in a my_array
def any_false(maybe_false, my_array):
    return any([not maybe_false(element) for element in my_array])


def get_logger(application_name: str, debug=DEBUG):
    """setup simultaneous logging to /tmp and to stdout"""
    logger = logging.getLogger(application_name)
    logger.propagate = False
    level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(level)
    if not logger.hasHandlers():
        file_name = os.path.basename(inspect.stack()[-1][1]).replace(".py", "")
        fh = logging.FileHandler(f"/tmp/{file_name}.{application_name}.log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
    return logger


### Natural Sorting


def atof(text):
    """opportunistic float conversion"""
    try:
        return float(text)
    except:
        return text


def natural_key(text):
    return [atof(c) for c in re.split(r"[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)", text)]


def natural_sort(lst):
    return sorted(lst, key=natural_key)


### Process Control and Task Creation


def multi_spawner(function_or_coroutine, cpu_index=0, forever=False):
    """ distributes work across separate processes running on optionally specified cpu cores.
    work can be provided as either a function or a coroutine and if a coroutine will be opportunistically run with uvloop."""

    def callable_wrapper(target_function_or_coroutine, forever=False):
        if faulthandler != None:
            faulthandler.register(signal.SIGUSR1)
        func_name = str(target_function_or_coroutine).split()[1]
        file_name = os.path.basename(inspect.stack()[-1][1]).replace(".py", "")
        logger = get_logger(f"{func_name}")
        try:
            cpu_count = os.cpu_count()
            cpu_map = [0] * cpu_count
            cpu_map[cpu_index] = 1
            os.sched_setaffinity(0, cpu_map)
        except:
            logging.debug("setaffinity failed")
        if isinstance(target_function_or_coroutine, functools.partial):
            actual_function = target_function_or_coroutine.func
        else:
            actual_function = target_function_or_coroutine
        try:
            if asyncio.iscoroutinefunction(actual_function):
                if uvloop:
                    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                loop = asyncio.get_event_loop()
                accepts_loop = "loop" in inspect.getfullargspec(actual_function).args
                if accepts_loop:
                    task = loop.create_task(target_function_or_coroutine(loop=loop))
                else:
                    task = loop.create_task(target_function_or_coroutine())
                res = loop.run_until_complete(task)
            else:
                loop = None
                res = target_function_or_coroutine()
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            logger.error("".join(traceback.format_exc()))
            return None
        if forever and loop:
            try:
                loop.run_forever()
            except KeyboardInterrupt:
                pass
            loop.close()
        return res

    process = multiprocessing.Process(
        target=callable_wrapper, args=(function_or_coroutine, forever)
    )
    process.start()
    return process


def native_spawn(
    x,
    timeout=30,
    no_return=False,
    loop=False,
    namespace=None,
    stdin_input=None,
    linker=False,
):
    """ carefully spawn a native command in an optional namespace, returning the contents of stdout and the process """
    if isinstance(x, str):
        x = [x]
    maybe_exec = which(x[0])
    if maybe_exec != None:
        x[0] = maybe_exec
    else:
        raise Exception("%s not found" % x[0])
    if "/usr/" not in maybe_exec:
        env = {
            "LD_LIBRARY_PATH": "/usr/local/lib:"
            + ":".join(
                (
                    os.getcwd() + "/lib",
                    "/".join(maybe_exec.replace("/bin/", "/lib/", 1).split("/")[:-1]),
                )
            )
        }
    else:
        env = {}
    env["LANG"] = "en_US.UTF-8"
    if linker:
        x = [linker] + x
    logging.debug("spawning: " + " ".join(x))
    if namespace and pyroute2:
        Popener = functools.partial(pyroute2.NSPopen, namespace)
    else:
        Popener = subprocess.Popen
    proc = Popener(
        x,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    if no_return:
        return None, proc
    else:
        try:
            stdout = proc.communicate(stdin_input, timeout=timeout)[0].decode()
            logging.debug("got stdout: " + stdout)
        except subprocess.TimeoutExpired:
            print(proc.pid, proc, "timeout expired")
            os.kill(proc.pid, signal.SIGKILL)
            stdout = None
        if pyroute2 and isinstance(proc, pyroute2.NSPopen):
            proc.release()
        return stdout, proc


def forever_spawn(arg, min_timeout=10, **kwargs):
    """ spawn a task using native_spawn as often as possible, unless it exits in less than min_timeout seconds """

    def task():
        start = time.time()
        while True:
            native_spawn(arg, **kwargs)[1].wait()
            now = time.time()
            if (now - start) < min_timeout:
                logging.debug("less than 10 seconds between loop ticks, breaking")
                break
            else:
                logging.debug("%f seconds since last loop tick" % (now - start))
                start = now

    run_forever = multiprocessing.Process(target=task)
    run_forever.start()
    return None, run_forever


def which(program_name):
    """ returns the full path to an executable with a given program_name if in a folder in os.environ['PATH'] """

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program_name)
    if fpath:
        if is_exe(program_name):
            return program_name
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program_name)
            if is_exe(exe_file):
                return exe_file
    return None


class DumbRing(object):
    """ a simple hash ring, using 'hasher' defined above. """

    def __init__(self, nodes=None):
        if not len(nodes):
            raise Exception
        self.ring = dict(enumerate(nodes))
        self.nodes = nodes
        self.ring_len = len(nodes)

    def get_node(self, string_key):
        if self.ring_len == 1:
            return self.nodes[0]
        return self.ring[hasher(string_key) % self.ring_len]


### Network Namespace Abstractions


@functools.lru_cache(maxsize=1024)
def get_fd_from_netnspath(netnspath):
    if "/" not in netnspath:
        netnspath = "/var/run/netns/%s" % netnspath
    return os.open(netnspath, os.O_RDONLY)


def set_ns(path):
    return pyroute2.netns.setns(get_fd_from_netnspath(path))


def get_nses():
    return natural_sort(os.listdir("/var/run/netns"))


async def fixed_open_connection(
    host=None,
    port=None,
    connected_socket=None,
    connect_timeout=10,
    loop=None,
    limit=65536,
    **kwds,
):
    """ fixes some implementation quirks present in asyncio.open_connection, offers ~ the same API """
    loop = loop or asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    if host and port and not connected_socket:
        request_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        request_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        request_socket.setblocking(False)
        try:
            await asyncio.wait_for(
                loop.sock_connect(request_socket, (host, port)), connect_timeout
            )
        except (
            asyncio.TimeoutError,
            RuntimeError,
            ConnectionError,
            OSError,
            asyncio.streams.IncompleteReadError,
        ):
            logging.debug("connect failed: %s:%d" % (host, port))
            request_socket = None
    if connected_socket and not host and not port:
        request_socket = sock
    if not request_socket:
        return None, None
    transport, _ = await loop.create_connection(
        lambda: protocol, sock=request_socket, **kwds
    )
    # fix buffer issues?
    transport.set_write_buffer_limits(0)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


### Redis functions


class CreateVerboseRedis:
    def __init__(self, underlying_connection, verbose=DEBUG, loop=None, max_len=64):
        self.loop = loop
        self.verbose = verbose
        self.underlying_connection = underlying_connection
        self.logger = get_logger("VerboseRedis", debug=DEBUG)
        self.MAX_LEN = max_len

    def __getattribute__(self, attrname: str) -> Callable[..., Any]:
        try:
            return object.__getattribute__(self, attrname)
        except AttributeError:
            pass

        def truncate(thing: str) -> str:
            if len(thing) > self.MAX_LEN:
                return f"{thing[:self.MAX_LEN]}...[{len(thing)-self.MAX_LEN} omitted]"
            else:
                return thing

        async def do_work_with_logging(*args, **kwargs):
            if self.verbose:
                stack = inspect.stack()
                frame_index = 0
                if len(stack) > 1:
                    frame_index += 1
                frame = stack[frame_index]
                while CURRENT_FILE.split("/")[-1] in stack[frame_index].filename:
                    if frame_index < len(stack):
                        frame_index += 1
                        frame = stack[frame_index]
                    else:
                        break
                if frame.code_context and len(frame.code_context):
                    context = frame.code_context[0].strip()
                else:
                    context = None
                machine_readable_stack_frame = dict(
                    filename=frame.filename,
                    lineno=frame.lineno,
                    function=frame.function,
                    context=context,
                )
            else:
                machine_readable_stack_frame = {}
            resp = await self.underlying_connection.__getattribute__(attrname)(
                *args, **kwargs
            )
            reprs = dict(
                args=[truncate(f"{a}") for a in args],
                kwargs=kwargs,
                resp=truncate(f"{resp}"),
            )
            kwargs_msg = f', kwargs: {reprs["kwargs"]}' if kwargs else ""
            self.logger.debug(
                f"{machine_readable_stack_frame}: called {attrname}() with args: {reprs['args']}{kwargs_msg}, got {reprs['resp']}"
            )
            return resp

        return do_work_with_logging


async def init_redis(redis_socket_path="/tmp/redis.sock", encoding=None):
    """ creates an aioredis connection to a redis instance listening on a specified unix socket """
    if not aioredis:
        raise Exception("aioredis not found!")
    loop = asyncio.get_event_loop()
    redis_connection = await aioredis.create_redis(
        redis_socket_path, loop=loop, encoding=encoding
    )
    return CreateVerboseRedis(redis_connection, verbose=True, loop=loop)


def start_redis_server(redis_socket_path="/tmp/redis.sock"):
    """ configures and launches an ephemeral redis instance listening at a specified redis_socket_path """
    conf = f"""port 0
databases 1
unixsocket {redis_socket_path}
maxmemory 100mb
maxmemory-policy volatile-lru
save ''
pidfile {redis_socket_path}.pid
daemonize yes""".encode()
    if not (
        os.path.exists(redis_socket_path + ".pid")
        and os.path.exists(f"/proc/{open(redis_socket_path+'.pid').read().strip()}")
    ):
        resp, proc = native_spawn(["redis-server", "-"], stdin_input=conf, timeout=5)
        atexit.register(proc.terminate)
    else:
        return True
    return proc


### UX / UI helpers


def visual_sleep(sleep_duration, message=""):
    """ time.sleep, but with a bargraph and an optional message """
    toolbar_width = 60
    sys.stdout.write("sleeping for %d seconds, %s\n" % (sleep_duration, message))
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width + 1))  # return to start of line, after '['
    for i in range(toolbar_width):
        time.sleep(sleep_duration / toolbar_width)
        sys.stdout.write("█")
        sys.stdout.flush()
    time.sleep(sleep_duration / toolbar_width)
    sys.stdout.write("\n")


krng = open("/dev/urandom", "rb")


def r1dx(x=20):
    """ returns a random, fair integer from 1 to X as if rolling a dice with the specified number of sides """
    max_r = 256
    assert x <= max_r
    while True:
        # get one byte, take as int on [1,256]
        r = int.from_bytes(krng.read(1), "little") + 1
        # if byte is less than the max factor of 'x' on the interval max_r, return r%x+1
        if r < (max_r - (max_r % x) + 1):
            return (r % x) + 1


def shuffled(l_list):
    return sorted(l_list, key=lambda x: krng.read(1))


# TCP/IP/HTTP functions

ip_regex = re.compile("(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")


async def get_ip_from_host(hostname="ipinfo.io"):
    if "." not in hostname:
        raise ConnectionError("malformed hostname")
    reader, writer = await fixed_open_connection(
        "1.0.0.1", 443, ssl=True, server_hostname="1.0.0.1"
    )
    if not reader:
        return
    writer.write(
        f"GET /dns-query?ct=application/dns-json&name={hostname}&type=A HTTP/1.1\r\nHost: 1.0.0.1\r\nConnection: close\r\n\r\n".encode()
    )
    headers = await reader.readuntil(b"\r\n\r\n")
    status, resp, tail = parse_headers(headers)
    length = int(resp[b"content-length"])
    answer_bytes = await asyncio.wait_for(reader.readexactly(length), 5.0)
    writer.close()
    out = json.loads(answer_bytes.decode())
    if ("Answer" in out.keys()) and (len(out["Answer"]) > 0):
        for maybe_answer in out["Answer"]:
            maybe_ip = maybe_answer["data"]
            if ip_regex.match(maybe_ip):
                return maybe_ip
    elif ("Answer" not in out.keys()) and out["Status"] == 0:
        return True
    elif out["Status"] == 3:
        return False
    else:
        return


def parse_headers(message):
    # 3x faster than http.client.parse_headers
    try:
        header_end = message.index(b"\r\n\r\n")
        headers = [
            z.split(b": ", 1)
            for z in message[0:header_end].split(b"\n")
            if z and b": " in z
        ]
        headers = dict([(x[0].lower().strip(), x[1].strip()) for x in headers])
        return message.split(b"\r\n", 1)[0], headers, message[header_end + 4 : :]
    except:
        raise Exception(("Couldn't parse HTTP headers in message. %s" % message))
