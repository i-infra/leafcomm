from node_core import *
import ssl
import json
import aiohttp
import functools as f
import handlebars
import _constants
from aiohttp import web
import logging
import nacl

import user_datastore

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
    # TODO: generalize this for multiple versions
    connection = request.app['redis']
    posted_bytes = await request.read()
    assert posted_bytes[0] == 1
    pubkey_bytes = posted_bytes[1:PUBKEY_SIZE+1]
    packer, unpacker = get_packer_unpacker(connection, pubkey_bytes, local_privkey_bytes = _constants.upstream_privkey_bytes)
    uid = await unpacker(posted_bytes)
    status = b'\x01'
    encrypted_status = await packer(status)
    await connection.hset('pubkey_uid_mapping', pubkey_bytes, uid)
    await connection.hset('first_seen_pubkey', pubkey_bytes, time.time())
    return web.Response(body=encrypted_status, status=200)

async def redistribute(loop=None):
    redis_connection = await init_redis("proxy")
    unpackers = {}
    async for ts, udp_bytes in pseudosub(redis_connection, 'udp_inbound'):
        if udp_bytes:
            assert udp_bytes[0] == 1
            pubkey_bytes = udp_bytes[1:PUBKEY_SIZE+1]
            if pubkey_bytes not in unpackers.keys():
                packer, unpacker = get_packer_unpacker(redis_connection, pubkey_bytes, local_privkey_bytes = _constants.upstream_privkey_bytes)
                unpackers[pubkey_bytes] = unpacker
            else:
                unpacker = unpackers[pubkey_bytes]
            uid = await redis_connection.hget('pubkey_uid_mapping', pubkey_bytes)
            update_msg = await unpacker(udp_bytes)
            logger.info(f'{uid.hex()} {update_msg}')
            await pseudopub(redis_connection, [uid], None, update_msg)

async def init_ws(request):
    redis_connection = await init_redis("proxy")
    logging.debug('ws request started')
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    access_token = await ws.receive()
    # TODO abstract this away, ws should receive an encrypted message prefixed with a session ID,
    # use the session ID to look up the pubkey and node uid, and try and decrypt the message
    uid = access_token.data
    json_bytes = await redis_connection.hget('most_recent', uid)
    if json_bytes is not None:
        await ws.send_str(json_bytes.decode())
    async for ts, update_msg in pseudosub(redis_connection, uid, timeout=_constants.websocket_update_period):
        if update_msg:
            json_bytes = json.dumps(update_msg)
            await redis_connection.hset('most_recent', uid, json_bytes)
        json_bytes = await redis_connection.hget('most_recent', uid)
        await ws.send_str(json_bytes.decode())
    return ws

async def start_authenticated_session(request):
    connection = request.app['redis']
    users_db = request.app['users_db']
    posted_bytes = await request.read()
    assert posted_bytes[0] == 1
    pubkey_bytes = posted_bytes[1:PUBKEY_SIZE+1]
    packer, unpacker = get_packer_unpacker(connection, pubkey_bytes, local_privkey_bytes=_constants.upstream_privkey_bytes)
    msg = await unpacker(posted_bytes)
    if 'signup' in request.rel_url.path:
        signup_required_fields = 'email name nodeSecret passwordHash passwordHint phone'.split()
        for key in signup_required_fields:
            assert msg.get(key) is not None
        user_exists_check = users_db.check_user(msg.get('email'), msg.get('passwordHash'))
        if user_exists_check == user_datastore.status_enum.index('NO_SUCH_USER'):
            success = users_db.add_user(msg.get('name'), msg.get('email'), msg.get('phone'), msg.get('passwordHash'), msg.get('passwordHint'), msg.get('nodeSecret'))
        msg = ('signup', user_datastore.status_enum[user_exists_check])
    if 'login' in request.rel_url.path:
        login_required_fields = ['email', 'passwordHash']
        for key in login_required_fields:
            assert msg.get(key) is not None
        user_exists_check = users_db.check_user(msg.get('email'), msg.get('passwordHash'))
        msg = ('login', user_datastore.status_enum[user_exists_check])
    encrypted_message = await packer(msg)
    return web.Response(body=encrypted_message, content_type='application/octet-stream')

def create_app(loop):
    async def start_background_tasks(app):
        app['redis'] = await init_redis("proxy")
        protocol = ProxyDatagramProtocol(loop, await init_redis("proxy"))
        app['udp_task'] = loop.create_task(loop.create_datagram_endpoint(lambda: protocol, local_addr=('0.0.0.0', _constants.upstream_port)))
        app['redistribute_task'] = loop.create_task(redistribute())
        app['users_db'] = user_datastore.UserDatabase(db_name=f'{data_dir}/users_db.db')
    app = web.Application()
    app.router.add_get('/ws', init_ws)
    app.router.add_post('/register', register_node)
    app.router.add_post('/signup', start_authenticated_session)
    app.router.add_post('/login', start_authenticated_session)
    app.on_startup.append(start_background_tasks)
    return app

if __name__ == "__main__":
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    redis_server_process = handlebars.start_redis_server(redis_socket_path=data_dir+'sproutwave.sock')
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
