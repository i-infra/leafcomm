import time
import cbor
import nacl
import nacl.public
import ulid2

import handlebars

PUBKEY_SIZE = nacl.public.PublicKey.SIZE
COUNTER_WIDTH = 4
data_tag_label = "__"
redis_prefix = "sproutwave"

async def tick(connection, function_name_depth=1, key=f'{redis_prefix}_function_call_ticks'):
    name = handlebars.get_function_name(function_name_depth)
    await connection.hset(key, name, time.time())


async def get_ticks(connection=None, key=f'{redis_prefix}_function_call_ticks'):
    if connection == None:
        connection = await init_redis()
    resp = await connection.hgetall(key)
    return {x: float(y) for (x, y) in resp.items()}


async def pseudopub(connection, channels, timestamp=None, reading=None):
    if isinstance(channels, str):
        channels = [channels]
    if not timestamp:
        timestamp = time.time()
    ulid = ulid2.generate_ulid_as_base32(timestamp=timestamp)
    data_tag = f"{data_tag_label}{ulid}"
    await connection.set(data_tag, cbor.dumps(reading))
    for channel in channels:
        await connection.lpush(f'{redis_prefix}_{channel}', data_tag)
    await connection.expireat(data_tag, int(timestamp + 600))
    return data_tag


async def pseudosub(connection, channel, timeout=360):
    while True:
        res = await pseudosub1(connection, channel, timeout)
        if res and res.value:
            yield res

class SerializedReading:
    def __init__(self, ulid, cbor_bytes):
        if isinstance(ulid, bytes):
            self.ulid = ulid.decode()
        else:
            self.ulid = ulid
        assert isinstance(self.ulid, str)
        if cbor_bytes:
            self.value = cbor.loads(cbor_bytes)
        else:
            self.value = None
        self.timestamp = ulid2.get_ulid_timestamp(self.ulid)

async def pseudosub1(connection, channel, timeout=360, depth=3, do_tick=True):
    channel, data_tag = await connection.brpop(f'{redis_prefix}_{channel}', timeout)
    obj_bytes = await connection.get(data_tag)
    ulid = data_tag.replace(data_tag_label.encode(), b'', 1)
    if do_tick == True:
        await tick(connection, function_name_depth=depth)
    return SerializedReading(ulid, obj_bytes)


async def get_next_counter(counter_name, redis_connection):
    current_value = await redis_connection.hincrby(f'{redis_prefix}_message_counters', counter_name, 1)
    return current_value


async def check_counter(counter_name, redis_connection, current_value):
    last_counter = await redis_connection.hget(f'{redis_prefix}_message_counters', counter_name)
    if not last_counter:
        last_counter = 0
    if int(last_counter) < current_value:
        await redis_connection.hset(f'{redis_prefix}_message_counters', counter_name, current_value)
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
