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

class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.loop.create_task(pseudopub(self.connection, ['udp_inbound'], None, data))

async def init_ws(request):
    logging.debug('ws request started')
    redis_connection = request.app['connection']
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    msg = await ws.receive()
    uid = msg.data
    pub_key_bytes = await redis_connection.hget('identities', uid)
    json_bytes = await redis_connection.hget('most_recent', uid)
    if json_bytes is not None:
        await ws.send_str(json_bytes)
    pub_key = nacl.public.PublicKey(pub_key_bytes)
    box = nacl.public.Box(relay_secret_key, pub_key)
    len_queue = await redis_connection.llen('udp_inbound')
    await redis_connection.ltrim('udp_inbound', len_queue-2, len_queue)
    while True:
        ws_closed = False
        ts, udp_bytes = await pseudosub1(redis_connection, 'udp_inbound', do_tick=False)
        if udp_bytes:
            try:
                compressed_cbor_bytes = box.decrypt(udp_bytes)
                update_msg = cbor.loads(zlib.decompress(compressed_cbor_bytes))
                print(update_msg)
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
        if json_bytes is not None:
            await redis_connection.hset('most_recent', uid, json_bytes)
            await ws.send_str(json_bytes)
    print('ws broke loop')
    return ws

def unbox_semisealed(byte_blob, secret_key = relay_secret_key):
    ppk = byte_blob[:32]
    pk = nacl.public.PublicKey(ppk)
    boxed_contents = byte_blob[32:]
    return ppk, nacl.public.Box(secret_key, pk).decrypt(boxed_contents)

async def register_node(request):
    connection = request.app['connection']
    posted_bytes = await request.read()
    pubkey_bytes, uid = unbox_semisealed(posted_bytes)
    await connection.hset('identities', uid, pubkey_bytes)
    return web.Response(status=200)

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
    app['connection'] = loop.run_until_complete(asyncio.ensure_future(init_redis("proxy")))
    protocol = ProxyDatagramProtocol(loop, loop.run_until_complete(asyncio.ensure_future(init_redis("proxy"))))
    loop.create_task(loop.create_datagram_endpoint(lambda: protocol, local_addr=('0.0.0.0', 8019)))
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=local_dir+"/resources/fullchain.pem", keyfile=local_dir+"/resources/privkey.pem")
    context.set_ciphers('RSA')
    web.run_app(app, host = '0.0.0.0', port = 8019, ssl_context=context)
