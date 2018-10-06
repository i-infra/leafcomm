from node_core import *
import ssl
import zlib
import json
import aiohttp
import functools as f
import handlebars
import _constants
from aiohttp import web
import logging
import nacl.public

logging.getLogger().setLevel(logging.DEBUG)

relay_secret_key = nacl.public.PrivateKey(_constants.upstream_privkey_bytes)

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
    connection = request.app['redis']
    posted_bytes = await request.read()
    pubkey_bytes, uid = unbox_semisealed(posted_bytes)
    session_id = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
    await connection.hset('session_pubkey_mapping', session_id, pubkey_bytes)
    await connection.hset('session_uid_mapping', session_id, uid)
    return web.Response(body=session_id, status=200)

async def redistribute(loop=None):
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

async def init_ws(request):
    redis_connection = await init_redis("proxy")
    logging.debug('ws request started')
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    access_token = await ws.receive()
    print(access_token)
    uid = access_token.data
    json_bytes = await redis_connection.hget('most_recent', uid)
    if json_bytes is not None:
        await ws.send_str(json_bytes.decode())
    while True:
        ts, update_msg = await pseudosub1(redis_connection, uid, timeout=1, do_tick=False)
        if update_msg:
            json_bytes = json.dumps(update_msg)
            await ws.send_str(json_bytes)
            await redis_connection.hset('most_recent', uid, json_bytes)
    return ws

def unbox_semisealed(byte_blob, secret_key = relay_secret_key):
    raw_pubkey = byte_blob[:PUBKEY_SIZE]
    nacl_public_key = nacl.public.PublicKey(raw_pubkey)
    boxed_contents = byte_blob[PUBKEY_SIZE:]
    contents = nacl.public.Box(secret_key, nacl_public_key).decrypt(boxed_contents)
    return raw_pubkey, contents

async def signup(request):
    connection = request.app['redis']
    posted_bytes = await request.read()
    msg = cbor.loads(posted_bytes)
    return web.json_response(msg)

def create_app(loop):
    async def start_background_tasks(app):
        app['redis'] = await init_redis("proxy")
        protocol = ProxyDatagramProtocol(loop, await init_redis("proxy"))
        app['udp_task'] = loop.create_task(loop.create_datagram_endpoint(lambda: protocol, local_addr=('0.0.0.0', _constants.upstream_port)))
        app['redistribute_task'] = loop.create_task(redistribute())
    app = web.Application()
    app.router.add_get('/ws', init_ws)
    app.router.add_post('/register', register_node)
    app.router.add_post('/signup', signup)
    app.on_startup.append(start_background_tasks)
    return app

if __name__ == "__main__":
    diag()
    handlebars.start_redis_server(redis_socket_path=data_dir+'proxysproutwave.sock')
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    if _constants.upstream_protocol == 'https':
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=local_dir+"/resources/fullchain.pem", keyfile=local_dir+"/resources/privkey.pem")
        context.set_ciphers('RSA')
    else:
        context = None
    web.run_app(app, host = '0.0.0.0', port = _constants.upstream_port, ssl_context=context)
