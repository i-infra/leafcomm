import asyncio
import base64
import errno
import functools
import inspect
import logging
import multiprocessing
import os
import re
import signal
import subprocess
import sys
import time
import traceback
from typing import Any, Callable

import aiohttp
import aiohttp.web
import aioredis
import blosc
import cbor
import nacl
import nacl.hash
import nacl.public
import numpy
import ulid2

import _constants
import cacheutils

PUBKEY_SIZE = nacl.public.PublicKey.SIZE
COUNTER_WIDTH = 4
CURRENT_PROTOCOL_VERSION = 1
data_tag_label = "__"
redis_prefix = "sproutwave"
data_dir = os.path.expanduser("~/.sproutwave/")
DEFAULT_REDIS_SOCKET_PATH = f"{data_dir}/{redis_prefix}.sock"
EXIT_VALUE = b"EXIT"
EXIT_KEY = f"{redis_prefix}_exit"
uvloop = None

DEBUG = bool(os.getenv("DEBUG") or "--debug" in sys.argv)
VERBOSE_REDIS = bool(os.getenv("VERBOSE_REDIS") or "--verbose_redis" in sys.argv)
CURRENT_FILE = globals().get("__file__") or ""


get_function_name = lambda depth=0: sys._getframe(depth + 1).f_code.co_name
ip_regex = re.compile("(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")
linker = [
    x for x in open("/proc/%s/maps" % os.getpid()).read().split("\n") if "ld" in x
][0].split()[-1]


def compress(in_):
    return dict(
        size=in_.size,
        dtype=in_.dtype.str,
        data=blosc.compress_ptr(
            in_.__array_interface__["data"][0],
            in_.size,
            in_.dtype.itemsize,
            clevel=1,
            shuffle=blosc.BITSHUFFLE,
            cname="lz4",
        ),
    )


def decompress(size: int, dtype: str, data: bytes, **kwargs) -> numpy.ndarray:
    out = numpy.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__["data"][0])
    return out


def any_false(maybe_false, my_array):
    return any([not maybe_false(element) for element in my_array])


def get_logger(application_name: str = None, debug=DEBUG):
    if application_name == None:
        application_name = get_function_name(1)
    "setup simultaneous logging to /tmp and to stdout"
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


def atof(text):
    "opportunistic float conversion"
    try:
        return float(text)
    except:
        return text


def natural_key(text):
    return [atof(c) for c in re.split("[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)", text)]


def natural_sort(lst):
    return sorted(lst, key=natural_key)


def multi_spawner(function_or_coroutine, cpu_index=0, forever=False):
    " distributes work across separate processes running on optionally specified cpu cores.\n    work can be provided as either a function or a coroutine and if a coroutine will be opportunistically run with uvloop."

    def callable_wrapper(target_function_or_coroutine, forever=False):
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
                res = asyncio.run(target_function_or_coroutine())
            else:
                loop = None
                res = target_function_or_coroutine()
        except Exception as e:
            ex_type, ex, tb = sys.exc_info()
            logger.error("".join(traceback.format_exc()))
            return None
        return res

    process = multiprocessing.Process(
        target=callable_wrapper, args=(function_or_coroutine, forever)
    )
    process.start()
    return process


def native_spawn(
    x, timeout=30, no_return=False, loop=False, stdin_input=None, linker=False,
):
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
    Popener = subprocess.Popen
    proc = Popener(
        x,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    if no_return:
        return (None, proc)
    else:
        try:
            stdout = proc.communicate(stdin_input, timeout=timeout)[0].decode()
            logging.debug("got stdout: " + stdout)
        except subprocess.TimeoutExpired:
            print(proc.pid, proc, "timeout expired")
            os.kill(proc.pid, signal.SIGKILL)
            stdout = None
        return (stdout, proc)


def which(program_name):
    " returns the full path to an executable with a given program_name if in a folder in os.environ['PATH'] "

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


class CreateVerboseRedis:
    def __init__(
        self, underlying_connection, verbose=VERBOSE_REDIS, loop=None, max_len=64
    ):
        self.loop = loop
        self.verbose = verbose
        self.underlying_connection = underlying_connection
        self.logger = get_logger("VerboseRedis", debug=VERBOSE_REDIS)
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


async def init_redis(redis_socket_path=None,):
    redis_socket_path = redis_socket_path or DEFAULT_REDIS_SOCKET_PATH
    " creates an aioredis connection to a redis instance listening on a specified unix socket "
    if not aioredis:
        raise Exception("aioredis not found!")
    loop = asyncio.get_event_loop()
    redis_connection = await aioredis.create_redis(redis_socket_path, loop=loop)
    if VERBOSE_REDIS:
        return CreateVerboseRedis(redis_connection, verbose=VERBOSE_REDIS, loop=loop)
    else:
        return redis_connection


def pid_exists(pid):
    """Check whether pid exists in the current process table.
    UNIX only.
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError("invalid PID 0")
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


async def check_redis(redis_socket_path="/tmp/redis.sock"):
    try:
        redis_connection = await init_redis(redis_socket_path=redis_socket_path)
        await redis_connection.ping()
        return True
    except:
        return False


def start_redis_server(redis_socket_path=DEFAULT_REDIS_SOCKET_PATH):
    " configures and launches an ephemeral redis instance listening at a specified redis_socket_path "
    conf = f"""port 0
databases 1
unixsocket {redis_socket_path}
maxmemory 100mb
maxmemory-policy volatile-lru
save ''
pidfile {redis_socket_path}.pid
daemonize yes""".encode()
    pid_path = f"{redis_socket_path}.pid"
    already_running = False
    maybe_pid = None
    if os.path.exists(pid_path):
        maybe_pid = int(open(pid_path).read().strip())
        if pid_exists(maybe_pid):
            already_running = True
    if not already_running:
        already_running = asyncio.run(check_redis(redis_socket_path))
    if already_running:
        get_logger().info(
            f"redis-server already running at {redis_socket_path} with PID {maybe_pid}!"
        )
        return True
    else:
        resp, proc = native_spawn(["redis-server", "-"], stdin_input=conf, timeout=5)
        time.sleep(5)
        return proc


async def pseudopub(connection, channels, timestamp=None, reading=None, metadata=None):
    if isinstance(channels, str):
        channels = [channels]
    if not timestamp:
        timestamp = time.time()
    ulid = ulid2.generate_ulid_as_base32(timestamp=timestamp)
    data_tag = f"{data_tag_label}{ulid}"
    if isinstance(reading, numpy.ndarray):
        reading = compress(reading)
        reading["metadata"] = metadata
        reading["pseudopub_compressed"] = True
    encoded_reading = cbor.dumps(reading)
    await connection.set(data_tag, encoded_reading, expire=600)
    for channel in channels:
        await connection.lpush(f"{redis_prefix}_{channel}", data_tag)
    return data_tag


async def pseudosub1(connection, channel, timeout=360):
    maybe_value = await connection.brpop(
        f"{redis_prefix}_{channel}", EXIT_KEY, timeout=timeout
    )
    if maybe_value == None:
        return None
    else:
        channel, value = maybe_value
        if value == EXIT_VALUE:
            await connection.lpush(EXIT_KEY, EXIT_VALUE)
            return EXIT_VALUE
    obj_bytes = await connection.get(value)
    ulid = value.replace(data_tag_label.encode(), b"", 1)
    if obj_bytes != None:
        value = cbor.loads(obj_bytes)
    else:
        value = None
    metadata = None
    if (
        isinstance(value, dict)
        and "pseudopub_compressed" in value
        and value["pseudopub_compressed"] == True
    ):
        metadata = value["metadata"]
        value = decompress(**value)
    return SerializedReading(ulid, value, metadata)


async def tick_on_schedule(connection, timeout, **kwargs):
    yield
    async for x in pseudosub(connection, None, timeout, _depth_offset=1, **kwargs):
        yield x


async def pseudosub(
    connection, channel, timeout=360, _depth_offset=0, ignore_exit=False, do_ticks=True
):
    pending = set()
    caller_name = get_function_name(1 + _depth_offset)
    logger = get_logger(application_name=caller_name)
    while True:
        if timeout < 1:
            if not len(pending):
                pending = {asyncio.create_task(pseudosub1(connection, channel))}
            done, pending = await asyncio.wait(
                pending, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if len(done):
                task = done.pop()
                res = task.result()
            else:
                res = None
        else:
            res = await pseudosub1(connection, channel, timeout)
        if do_ticks == True:
            await connection.hset(f"{redis_prefix}_f_ticks", caller_name, time.time())
        if res == EXIT_VALUE and not ignore_exit:
            logger.info(f"{channel}: quitting")
            break
        else:
            yield res


class SerializedReading:
    def __init__(self, ulid, value, metadata=None):
        if isinstance(ulid, bytes):
            self.ulid = ulid.decode()
        else:
            self.ulid = ulid
        assert isinstance(self.ulid, str)
        self.value = value
        self.timestamp = ulid2.get_ulid_timestamp(self.ulid)
        self.metadata = metadata


async def get_next_counter(counter_name, redis_connection):
    current_value = await redis_connection.hincrby(
        f"{redis_prefix}_message_counters", counter_name, 1
    )
    return current_value


class CounterException(aiohttp.web.HTTPUnprocessableEntity):
    def __init__(self, message, errors):
        self.message = message
        self.errors = errors
        super().__init__(text=f"Counter Error, {str(errors)}")


async def check_counter(counter_name, redis_connection, current_value):
    last_counter = int(
        (await redis_connection.hget(f"{redis_prefix}_message_counters", counter_name))
        or 0
    )
    if int(last_counter) < current_value:
        await redis_connection.hset(
            f"{redis_prefix}_message_counters", counter_name, current_value
        )
        return True
    else:
        raise CounterException(
            f"{counter_name} got invalid counter",
            (counter_name, current_value, last_counter + 1),
        )


packer_unpacker_cache = cacheutils.LRI(128)


async def unwrap_message(message_bytes, redis_connection=None):
    """ high level abstraction accepting a byte sequence and returning the pubkey and message.
    will attempt to reuse existing crypto intermediate values. """
    global packer_unpacker_cache
    redis_connection = redis_connection or await init_redis("proxy")
    pubkey_bytes = message_bytes[1 : PUBKEY_SIZE + 1]
    if pubkey_bytes not in packer_unpacker_cache.keys():
        packer, unpacker = await get_packer_unpacker(
            pubkey_bytes,
            local_privkey_bytes=_constants.upstream_privkey_bytes,
            redis_connection=redis_connection,
        )
        packer_unpacker_cache[pubkey_bytes] = (packer, unpacker)
    else:
        packer, unpacker = packer_unpacker_cache[pubkey_bytes]
    unpacked_message = await unpacker(message_bytes)
    return pubkey_bytes, unpacked_message


async def wrap_message(pubkey_bytes, message, redis_connection=None, b64=False):
    global packer_unpacker_cache
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](message)
    if b64:
        encrypted_message = base64.b64encode(encrypted_message)
    return encrypted_message


class AbbreviatedBase32Encoder:
    @staticmethod
    def encode(data):
        data_128bits = data[:16] + b"\x00" * (16 - len(data))
        return ulid2.encode_ulid_base32(data_128bits)

    def decode(data):
        return ulid2.decode_ulid_base32(data)


async def get_packer_unpacker(
    peer_pubkey_bytes, local_privkey_bytes=None, redis_connection=None
):
    # TODO: compression
    redis_connection = redis_connection or await init_redis()
    peer_public_key = nacl.public.PublicKey(peer_pubkey_bytes)
    logger = get_logger()
    if local_privkey_bytes is None:
        local_privkey = nacl.public.PrivateKey.generate()
    else:
        local_privkey = nacl.public.PrivateKey(local_privkey_bytes)
    session_box = nacl.public.Box(local_privkey, peer_public_key)
    local_pubkey_bytes = local_privkey.public_key.encode()
    counter_name = nacl.hash.sha256(
        local_pubkey_bytes + peer_pubkey_bytes, encoder=AbbreviatedBase32Encoder
    )

    async def packer(message):
        logger.debug(str(message))
        nonce = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
        counter = await get_next_counter(counter_name + "_packer", redis_connection)
        counter_bytes = counter.to_bytes(COUNTER_WIDTH, "little")
        cyphertext = session_box.encrypt(counter_bytes + cbor.dumps(message), nonce)
        return (
            CURRENT_PROTOCOL_VERSION.to_bytes(1, "little")
            + local_pubkey_bytes
            + cyphertext
        )

    async def unpacker(message):
        message_version = message[0]
        assert message_version == CURRENT_PROTOCOL_VERSION
        plaintext = session_box.decrypt(message[1 + PUBKEY_SIZE :])
        counter = int.from_bytes(plaintext[:COUNTER_WIDTH], "little")
        if await check_counter(counter_name + "_unpacker", redis_connection, counter):
            raw_msg = cbor.loads(plaintext[COUNTER_WIDTH:])
            logger.debug(str(raw_msg))
            return raw_msg

    return packer, unpacker


async def make_wrapped_http_request(aiohttp_client_session, packer, unpacker, url, msg):
    encrypted_msg = await packer(msg)
    logger = get_logger()
    async with aiohttp_client_session.post(url=url, data=encrypted_msg) as resp:
        if resp.status == 200:
            encrypted_response = await resp.read()
            if resp.content_type == "application/base64":
                encrypted_response = base64.b64decode(encrypted_response)
            unpacked = await unpacker(encrypted_response)
            return unpacked
        else:
            logger.error(f"{str(resp).strip()} : {await resp.text()}")
