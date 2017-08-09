import asyncio
import uuid
import base64
import logging
import zlib
import json

import aiohttp
from aiohttp import web

import cbor
import nacl.public
import asyncio_redis
from asyncio_redis.encoders import BaseEncoder

import aioudp

logging.getLogger().setLevel(logging.DEBUG)

class CborEncoder(BaseEncoder):
    native_type = object
    def encode_from_native(self, data):
        return cbor.dumps(data)
    def decode_to_native(self, data):
        return cbor.loads(data)

relay_key = nacl.public.PrivateKey(b'\x8fw\xadU\xb6\x0bD\xd1\xbf>Q\xfa\xf5>9\xc4)\x0b\x11\xc8\xf9\x0e\xa3\x14\x14~:\x87\xa4\x12\x15.')

class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        logging.debug(data)
        self.loop.create_task(submit_data(data, connection))

async def submit_data(data, connection):
    await connection.publish('udp_inbound', data)

async def init_ws(request):
    pubsub_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    udp_inbound_channel  = await pubsub_connection.start_subscribe()
    await udp_inbound_channel.subscribe(['udp_inbound'])
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    uid = None
    msg = await ws.receive()
    assert msg.type == aiohttp.WSMsgType.TEXT
    uid = str(msg.data)
    print('rxed on', msg.type, msg.data)
    pub_key_bytes = await redis_connection.hget('identities', uid)
    pub_key = nacl.public.PublicKey(pub_key_bytes)
    box = nacl.public.Box(relay_key, pub_key)
    while True:
        ws_closed = False
        udp_bytes = (await udp_inbound_channel.next_published()).value
        compressed_cbor_bytes = box.decrypt(udp_bytes[0])
        update_msg = cbor.loads(zlib.decompress(compressed_cbor_bytes))
        json_bytes = json.dumps(update_msg)
        print(json_bytes)
        try:
            msg = await asyncio.wait_for(ws.receive(), 1)
            print(msg)
            if msg.type in [aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING]:
                break
        except asyncio.TimeoutError:
            pass
        ws.send_str(json_bytes)
    return ws

async def register(request):
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    posted_bytes = await request.read()
    new_identity = nacl.public.SealedBox(relay_key).decrypt(posted_bytes)
    if new_identity is not None:
        uid, pubkey_bytes = tuple(cbor.loads(new_identity))
        identities_exists = await redis_connection.exists('identities')
        if identities_exists:
            uid_in_table = uid in await redis_connection.hkeys('identities')
        else:
            uid_in_table = False
        if not uid_in_table:
            logging.debug('set '+str(uid))
            await redis_connection.hset('identities', uid, pubkey_bytes)
        return web.Response(status=200)
    return web.Response(status=403)

def create_app(loop):
    app = web.Application()
    app['redis_connection'] = None
    app.router.add_get('/ws', init_ws)
    app.router.add_post('/register', register)
    return app

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    connection_fut = asyncio.ensure_future(asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder()))
    connection = loop.run_until_complete(connection_fut)
    protocol = ProxyDatagramProtocol(loop, connection)
    loop.create_task(loop.create_datagram_endpoint(
        lambda: protocol, local_addr=('0.0.0.0', 8019)))
    web.run_app(app, host = '127.0.0.1', port = 8081)
