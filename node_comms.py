import asyncio
import base64
import os
import sys
import time

import _constants
import aiohttp
import aiohttp.web
import cacheutils
import cbor
import handlebars
import nacl
import nacl.hash
import nacl.public
import ulid2

logger = handlebars.get_logger(__name__, debug="--debug" in sys.argv)

PUBKEY_SIZE = nacl.public.PublicKey.SIZE
COUNTER_WIDTH = 4
CURRENT_PROTOCOL_VERSION = 1
data_tag_label = "__"
redis_prefix = "sproutwave"

data_dir = os.path.expanduser("~/.sproutwave/")


def init_redis(preface=""):
    return handlebars.init_redis(data_dir + preface + "sproutwave.sock")




async def tick(connection, function_name_depth=1, key=f"{redis_prefix}_function_call_ticks"):
    name = handlebars.get_function_name(function_name_depth)
    await connection.hset(key, name, time.time())


async def get_ticks(connection=None, key=f"{redis_prefix}_function_call_ticks"):
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
        await connection.lpush(f"{redis_prefix}_{channel}", data_tag)
    await connection.expireat(data_tag, int(timestamp + 600))
    return data_tag


async def pseudosub(connection, channel, timeout=360):
    while True:
        res = await pseudosub1(connection, channel, timeout)
        if res and res.value != None:
            yield res


class SerializedReading:
    def __init__(self, ulid, cbor_bytes):
        if isinstance(ulid, bytes):
            self.ulid = ulid.decode()
        else:
            self.ulid = ulid
        assert isinstance(self.ulid, str)
        if cbor_bytes != None:
            self.value = cbor.loads(cbor_bytes)
        else:
            self.value = None
        self.timestamp = ulid2.get_ulid_timestamp(self.ulid)


async def pseudosub1(connection, channel, timeout=360, depth=3, do_tick=True):
    channel, data_tag = await connection.brpop(f"{redis_prefix}_{channel}", timeout)
    obj_bytes = await connection.get(data_tag)
    ulid = data_tag.replace(data_tag_label.encode(), b"", 1)
    if do_tick == True:
        await tick(connection, function_name_depth=depth)
    return SerializedReading(ulid, obj_bytes)


async def get_next_counter(counter_name, redis_connection):
    current_value = await redis_connection.hincrby(f"{redis_prefix}_message_counters", counter_name, 1)
    return current_value


class CounterException(aiohttp.web.HTTPUnprocessableEntity):
    def __init__(self, message, errors):
        self.message = message
        self.errors = errors
        super().__init__(text=f"Counter Error, {str(errors)}")


async def check_counter(counter_name, redis_connection, current_value):
    last_counter = int((await redis_connection.hget(f"{redis_prefix}_message_counters", counter_name)) or 0)
    if int(last_counter) < current_value:
        await redis_connection.hset(f"{redis_prefix}_message_counters", counter_name, current_value)
        return True
    else:
        raise CounterException(f"{counter_name} got invalid counter", (counter_name, current_value, last_counter + 1))


packer_unpacker_cache = cacheutils.LRI(128)


async def unwrap_message(message_bytes, redis_connection=None):
    """ high level abstraction accepting a byte sequence and returning the pubkey and message.
    will attempt to reuse existing crypto intermediate values. """
    global packer_unpacker_cache
    redis_connection = redis_connection or await init_redis("proxy")
    pubkey_bytes = message_bytes[1 : PUBKEY_SIZE + 1]
    if pubkey_bytes not in packer_unpacker_cache.keys():
        packer, unpacker = await get_packer_unpacker(pubkey_bytes, local_privkey_bytes=_constants.upstream_privkey_bytes, redis_connection=redis_connection)
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


async def get_packer_unpacker(peer_pubkey_bytes, local_privkey_bytes=None, redis_connection=None):
    # TODO: compression
    redis_connection = redis_connection or await init_redis()
    peer_public_key = nacl.public.PublicKey(peer_pubkey_bytes)
    if local_privkey_bytes is None:
        local_privkey = nacl.public.PrivateKey.generate()
    else:
        local_privkey = nacl.public.PrivateKey(local_privkey_bytes)
    session_box = nacl.public.Box(local_privkey, peer_public_key)
    local_pubkey_bytes = local_privkey.public_key.encode()
    counter_name = nacl.hash.sha256(local_pubkey_bytes + peer_pubkey_bytes, encoder=AbbreviatedBase32Encoder)

    async def packer(message):
        logger.debug(str(message))
        nonce = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
        counter = await get_next_counter(counter_name + "_packer", redis_connection)
        counter_bytes = counter.to_bytes(COUNTER_WIDTH, "little")
        cyphertext = session_box.encrypt(counter_bytes + cbor.dumps(message), nonce)
        return CURRENT_PROTOCOL_VERSION.to_bytes(1, "little") + local_pubkey_bytes + cyphertext

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
    async with aiohttp_client_session.post(url=url, data=encrypted_msg) as resp:
        if resp.status == 200:
            encrypted_response = await resp.read()
            if resp.content_type == "application/base64":
                encrypted_response = base64.b64decode(encrypted_response)
            unpacked = await unpacker(encrypted_response)
            return unpacked
        else:
            logger.error(f"{str(resp).strip()} : {await resp.text()}")
