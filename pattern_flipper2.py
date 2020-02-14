from node_core import *

FL2000_SAMPLE_RATE = 10_000_000
FPS = 10
SYMBOL_DURATION_SAMPLES = 750 // 3 * 10  # 750us per cluster of three bits
UID = 0x0323

_ZERO = [(SYMBOL_DURATION_SAMPLES * 1, 255), (SYMBOL_DURATION_SAMPLES * 2, 0)]

_ONE = [(SYMBOL_DURATION_SAMPLES * 2, 255), (SYMBOL_DURATION_SAMPLES * 1, 0)]
ON = 1
OFF = 0


def build_message_outlet_fl2000(outlet_code, state=OFF):
    bit_sequence = [x for x in outlet_code]
    if any([x not in (0, 1) for x in bit_sequence]):
        raise ValueError("outlet_code argument must be an iterable of 1s and 0s")
    state = int(bool(state))
    if state == OFF:
        bit_sequence[-5:] = [1, 1, 0, 0, 0]
    elif state == ON:
        bit_sequence[-5:] = [0, 0, 1, 1, 0]
    else:
        raise ValueError("state argument must be bool-like, ie 1/0 or True/False")
    blob = bytes(rld(rerle(flatten([{0: _ZERO, 1: _ONE}[i] for i in bit_sequence]))))
    blob += 12 * SYMBOL_DURATION_SAMPLES * b"\x00"
    blob *= 6
    # last repeat has bottom bits all zero, regardless of mode
    bit_sequence[-5:] = [0] * 5
    blob += bytes(rld(rerle(flatten([{0: _ZERO, 1: _ONE}[i] for i in bit_sequence]))))
    return blob


async def push_message_fl2000(bytes_, redis_connection=None):
    if not redis_connection:
        redis_connection = await init_redis()
    await pseudopub(redis_connection, "fl2000_bytes", None, bytes_)


async def start_flip_fl2000(reader, writer, loop, redis_connection):
    empty_frame = int(FL2000_SAMPLE_RATE * 1 / FPS) * b"\x00"
    high = len(empty_frame) * 2
    low = len(empty_frame)
    writer.transport.set_write_buffer_limits(high, low)
    start = time.time()
    last = start
    print(f"started at {start}")
    total = 0
    rate = FL2000_SAMPLE_RATE
    timeout = 0.8 * 1 / FPS  # use pseudosub to soak up 80% of the time between frames
    for _ in range(2):
        writer.write(empty_frame)
        await writer.drain()
    async for message in pseudosub(redis_connection, "fl2000_bytes", timeout=timeout, ignore_exit = True, do_ticks = False):
        blob = empty_frame
        now = time.time()
        if isinstance(message, SerializedReading):
            print("got data from", now - message.timestamp, "ago")
            blob = message.value
            print(f"writing {len(blob)} bytes")
        if isinstance(blob, numpy.ndarray) and (blob.dtype == numpy.uint8):
            blob = blob.tobytes()
        blob += b"\x00" * (len(empty_frame) - len(blob))
        # blob += empty_frame
        writer.write(blob)
        await writer.drain()
        total += len(blob)
        rate = total / (time.time() - start)
        after_write = time.time()
        if (after_write - last) > (1 / FPS):
            print(f"TimeoutError: {after_write-last} > {1/FPS}")
        time_remaining = 1 / FPS - (time.time() - last)
        if time_remaining > 0.001:
            await asyncio.sleep(time_remaining)
            print(rate, time_remaining, time.time() - last)
        last = now


async def start_fl2000_daemon(host="0.0.0.0", port=1235, forever=True):
    loop = asyncio.get_event_loop()
    redis_connection = await init_redis()
    wrapped_flip_cb = functools.partial(
        start_flip_fl2000, loop=loop, redis_connection=redis_connection
    )

    def factory():
        reader = asyncio.StreamReader(limit=2 ** 16, loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, wrapped_flip_cb, loop=loop)
        return protocol

    server = await loop.create_server(
        factory, host, port, reuse_address=True, reuse_port=True
    )
    if forever:
        await server.serve_forever()
    return loop, server


def main(args):
    redis_process = start_redis_server()
    asyncio.run(start_fl2000_daemon())


if __name__ == "__main__":
    args = sys.argv[1::]
    main(args)
