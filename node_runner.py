import atexit
import importlib
import inspect
import pathlib
import signal
import time

import xxhash

import node_controller
import node_core
import pattern_streamer
from node_comms import *

global LAST_EXIT
LAST_EXIT = 0


def time_to_exit(signalnum, frame):
    global LAST_EXIT
    now = time.time()
    if (now - LAST_EXIT) < 60:
        raise KeyboardInterrupt
    LAST_EXIT = time.time()

    async def indicate_exit():
        connection = await init_redis()
        await connection.lpush(EXIT_KEY, EXIT_VALUE)
        await connection.lpush(EXIT_KEY, EXIT_VALUE)
        await connection.lpush(EXIT_KEY, EXIT_VALUE)

    multi_spawner(indicate_exit).join()


def cleanup_exit():
    async def cleanup_exit():
        connection = await init_redis()
        await connection.delete(EXIT_KEY)

    multi_spawner(cleanup_exit).join()

async def get_ticks(connection=None, key=f"{redis_prefix}_f_ticks"):
    if connection == None:
        connection = await init_redis()
    resp = await connection.hgetall(key)
    return {x: float(y) for (x, y) in resp.items()}

async def main():
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGINT, time_to_exit)
    atexit.register(cleanup_exit)
    redis_server_process = start_redis_server()
    logger = get_logger()
    connection = await init_redis()
    funcs = (
        node_core.rtlsdr_threshold_and_accumulate,
        node_core.process_iq_readings,
        node_core.sample_to_datastore,
        node_core.sensor_monitor,
        node_controller.run_controllers,
        pattern_streamer.start_fl2000_daemon
    )
    function_process_mapping = {}
    file_hash_mapping = {}
    async for _ in tick_on_schedule(connection, timeout=120):
        now = time.time()
        function_ticks = await get_ticks(connection)
        for func in funcs:
            func_name = func.__name__
            func_file = inspect.getsourcefile(func)
            func_file_hash = xxhash.xxh64(open(func_file, "rb").read()).hexdigest()
            func_changed = func_file in file_hash_mapping and file_hash_mapping[func_file] != func_file_hash
            func_module = inspect.getmodulename(func_file)
            if func_name in function_process_mapping:
                func_alive = function_process_mapping[func_name].is_alive()
            else:
                func_alive = False
            file_hash_mapping[func_file] = func_file_hash
            logger.debug(f"{func_name} from {func_file} @ {func_file_hash}; alive: {func_alive}")
            if func_changed:
                logger.info(
                    f"reloading {func_name} from {func_file.split('/')[-1]} @ {func_file_hash}"
                )
                importlib.reload(func_module)
            if func_changed:
                function_process_mapping[func_name].terminate()
            if func_changed or not func_alive:
                function_process_mapping[func_name] = multi_spawner(func)


if __name__ == "__main__":
    asyncio.run(main())
