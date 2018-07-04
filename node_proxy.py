from node_core import *
import ssl
import zlib
import json
import aiohttp
import functools as f
from aiohttp import web

import nacl.public

logging.getLogger().setLevel(logging.DEBUG)

relay_secret_key = nacl.public.PrivateKey(b'\x8fw\xadU\xb6\x0bD\xd1\xbf>Q\xfa\xf5>9\xc4)\x0b\x11\xc8\xf9\x0e\xa3\x14\x14~:\x87\xa4\x12\x15.')


PUBKEY_SIZE = nacl.public.PublicKey.SIZE
SESSION_ID_SIZE = NONCE_SIZE = nacl.public.Box.NONCE_SIZE

class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.loop.create_task(pseudopub(self.connection, ['udp_inbound'], None, data))

async def register_node(request):
    connection = request.app['connection']
    posted_bytes = await request.read()
    pubkey_bytes, session_id, uid = unbox_semisealed(posted_bytes)
    print(pubkey_bytes, session_id, uid)
    await connection.hset('session_pubkey_mapping', session_id, pubkey_bytes)
    await connection.hset('session_uid_mapping', session_id, uid)
    return web.Response(status=200)

async def redistribute():
    redis_connection = await init_redis("proxy")
    boxes = {}
    async for ts, udp_bytes in pseudosub(redis_connection, 'udp_inbound'):
        if udp_bytes:
            session_id = udp_bytes[:SESSION_ID_SIZE]
            pubkey_bytes = await redis_connection.hget('session_pubkey_mapping', session_id)
            uid = await redis_connection.hget('session_uid_mapping', session_id)
            if pubkey_bytes and uid:
                if pubkey_bytes in boxes.keys():
                    box = boxes[pubkey_bytes]
                else:
                    pubkey = nacl.public.PublicKey(pubkey_bytes)
                    box = nacl.public.Box(relay_secret_key, pubkey)
                compressed_cbor_bytes = box.decrypt(udp_bytes[SESSION_ID_SIZE:])
                update_msg = cbor.loads(zlib.decompress(compressed_cbor_bytes))
                await pseudopub(redis_connection, [uid], None, update_msg)
                print(uid, update_msg)

async def init_ws(request):
    logging.debug('ws request started')
    redis_connection = request.app['connection']
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    msg = await ws.receive()
    uid = msg.data
    json_bytes = await redis_connection.hget('most_recent', uid)
    if json_bytes is not None:
        await ws.send_str(json_bytes)
    while True:
        ws_closed = False
        ts, update_msg = await pseudosub1(redis_connection, uid, do_tick=False)
        if update_msg:
            try:
                json_bytes = json.dumps(update_msg)
            except:
                json_bytes = None
        else:
            json_bytes = None
        try:
            msg = await asyncio.wait_for(ws.receive(), 1)
            if msg.type in [aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING]:
                print('closing')
                break
        except asyncio.TimeoutError:
            pass
        if json_bytes:
            await redis_connection.hset('most_recent', uid, json_bytes)
            await ws.send_str(json_bytes)
    print('ws broke loop')
    return ws

def unbox_semisealed(byte_blob, secret_key = relay_secret_key):
    raw_pubkey = byte_blob[:PUBKEY_SIZE]
    nacl_public_key = nacl.public.PublicKey(raw_pubkey)
    boxed_contents = byte_blob[PUBKEY_SIZE:]
    contents = nacl.public.Box(secret_key, nacl_public_key).decrypt(boxed_contents)
    session_id = contents[:NONCE_SIZE]
    value = contents[NONCE_SIZE:]
    return raw_pubkey, session_id, value

async def signup(request):
    connection = request.app['connection']
    posted_bytes = await request.read()
    msg = cbor.loads(posted_bytes)
    return web.json_response(msg)

def create_app(loop):
    app = web.Application()
    app['redis_connection'] = None
    app.router.add_get('/ws', init_ws)
    app.router.add_post('/register', register_node)
    app.router.add_post('/signup', signup)
    return app

if __name__ == "__main__":
    diag()
    redis_server = f.partial(start_redis_server, 'proxy')
    redis_server.__name__ = "redis_server"
    spawner(redis_server).join()
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    spawner(redistribute)
    app['connection'] = loop.run_until_complete(asyncio.ensure_future(init_redis("proxy")))
    protocol = ProxyDatagramProtocol(loop, loop.run_until_complete(asyncio.ensure_future(init_redis("proxy"))))
    loop.create_task(loop.create_datagram_endpoint(lambda: protocol, local_addr=('0.0.0.0', 8019)))
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=local_dir+"/resources/fullchain.pem", keyfile=local_dir+"/resources/privkey.pem")
    context.set_ciphers('RSA')
    web.run_app(app, host = '0.0.0.0', port = 8019, ssl_context=context)
