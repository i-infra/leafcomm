import asyncio
import uuid
import base64
import logging
import json

import aiohttp
from aiohttp import web

import cbor

ej = json.dumps
dj = json.loads

loop = asyncio.get_event_loop()

import nacl.public
import asyncio_redis
import aioudp
import phase1

relay_key = nacl.public.PrivateKey(b'\x8fw\xadU\xb6\x0bD\xd1\xbf>Q\xfa\xf5>9\xc4)\x0b\x11\xc8\xf9\x0e\xa3\x14\x14~:\x87\xa4\x12\x15.')

async def start_udp_server(loop, host, port):
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    logging.debug('udp task connected to redis')
    udp = await aioudp.open_local_endpoint(host=host, port=port, loop=loop)
    logging.debug('listening on udp port')
    while True:
        data = await udp.read()
        logging.debug(data)
        await redis_connection.publish('udp_inbound', data)

async def init_ws(request):
    pubsub_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    udp_inbound_channel  = await pubsub_connection.start_subscribe()
    await udp_inbound_channel.subscribe(['udp_inbound'])
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    uid = None
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            uid = str(msg.data)
            print('rxed on', msg.type, msg.data)
            pub_key_bytes = await redis_connection.hget('identities', uid)
            pub_key = nacl.public.PublicKey(pub_key_bytes) 
            box = nacl.public.Box(relay_key, pub_key)
            while True:
                udp_bytes = (await udp_inbound_channel.next_published()).value
                print(udp_bytes)
                cbor_bytes = box.decrypt(udp_bytes[0])
                update_msg = cbor.loads(cbor_bytes)
                json_bytes = json.dumps(update_msg)
                ws.send_str(json_bytes)
        if msg.type == web.MsgType.close:
            break
    return ws

async def register(request):
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    posted_bytes = await request.read()
    new_identity = nacl.public.SealedBox(relay_key).decrypt(posted_bytes)
    if new_identity is not None:
        uid, pubkey_bytes = tuple(cbor.loads(new_identity))
        identities_exists = await redis_connection.exists('identities')
        if identities_exists:
            uid_in_table = uid in await redis_connection.hkeys('identities')
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
    loop.create_task(start_udp_server(loop, 'localhost', 8019))
    web.run_app(app, host = '0.0.0.0', port = 8019)
