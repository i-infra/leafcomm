import cbor
import nacl
import nacl.public
import handlebars

import time

PUBKEY_SIZE = nacl.public.PublicKey.SIZE
COUNTER_WIDTH = 4


async def tick(connection, function_name_depth=1, key='sproutwave_ticks'):
    name = handlebars.get_function_name(function_name_depth)
    await connection.hset(key, name, time.time())


async def get_ticks(connection=None, key='sproutwave_ticks'):
    if connection == None:
        connection = await init_redis()
    resp = await connection.hgetall(key)
    return {x: float(y) for (x, y) in resp.items()}


async def pseudopub(connection, channels, timestamp=None, info=None):
    if isinstance(channels, str):
        channels = [channels]
    if not timestamp:
        timestamp = time.time()
    await connection.set(timestamp, cbor.dumps(info))
    for channel in channels:
        await connection.lpush(channel, timestamp)
    await connection.expireat(timestamp, int(timestamp + 600))


async def pseudosub(connection, channel, timeout=360):
    while True:
        res = await pseudosub1(connection, channel, timeout)
        if res:
            yield res


async def pseudosub1(connection, channel, timeout=360, depth=3, do_tick=True):
    channel, timestamp = await connection.brpop(channel, timeout)
    info = await connection.get(timestamp)
    if do_tick == True:
        await tick(connection, function_name_depth=depth)
    if info is not None:
        return (float(timestamp), cbor.loads(info))


async def get_next_counter(counter_name, redis_connection):
    current_value = await redis_connection.hincrby('message_counters', counter_name, 1)
    return current_value


async def check_counter(counter_name, redis_connection, current_value):
    last_counter = await redis_connection.hget('message_counters', counter_name)
    if not last_counter:
        last_counter = 0
    if int(last_counter) < current_value:
        await redis_connection.hset('message_counters', counter_name, current_value)
        return True
    else:
        raise Exception(f"{counter_name.hex()}: got {current_value} expected at least {int(last_counter)+1}")


def get_packer_unpacker(redis_connection, peer_pubkey_bytes, local_privkey_bytes=None):
    # TODO: compression
    version_tag = b'\x01'
    version_int = int.from_bytes(version_tag, 'little')
    peer_public_key = nacl.public.PublicKey(peer_pubkey_bytes)
    if local_privkey_bytes is None:
        local_privkey = nacl.public.PrivateKey.generate()
    else:
        local_privkey = nacl.public.PrivateKey(local_privkey_bytes)
    session_box = nacl.public.Box(local_privkey, peer_public_key)
    local_pubkey_bytes = local_privkey.public_key.encode()
    counter_name = local_pubkey_bytes + peer_pubkey_bytes

    async def packer(message):
        nonce = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
        counter = await get_next_counter(counter_name, redis_connection)
        counter_bytes = counter.to_bytes(COUNTER_WIDTH, 'little')
        cyphertext = session_box.encrypt(counter_bytes + cbor.dumps(message), nonce)
        return version_tag + local_pubkey_bytes + cyphertext

    async def unpacker(message):
        message_version = message[0]
        assert message_version == version_int
        plaintext = session_box.decrypt(message[1 + PUBKEY_SIZE:])
        counter = int.from_bytes(plaintext[:COUNTER_WIDTH], 'little')
        if await check_counter(counter_name, redis_connection, counter):
            return cbor.loads(plaintext[COUNTER_WIDTH:])

    return packer, unpacker
