from node_comms import *
import ssl
import json
import aiohttp
import functools as f
import handlebars
import _constants
from aiohttp import web
import logging
import asyncio
import nacl
import dataclasses
import user_datastore
import pathlib
import os
import sys
import base64

logger = handlebars.get_logger(__name__, debug='--debug' in sys.argv)

data_dir = os.path.expanduser('~/.sproutwave/')

import cacheutils

packer_unpacker_cache = cacheutils.LRI(128)

def init_redis(preface=''):
    return handlebars.init_redis(data_dir + preface + 'sproutwave.sock')


class ProxyDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.loop.create_task(pseudopub(self.connection, ['udp_inbound'], None, data))


async def unwrap_message(message_bytes, redis_connection = None, packer_unpacker_cache_ = {}):
    global packer_unpacker_cache
    redis_connection = redis_connection or await init_redis('proxy')
    assert message_bytes[0] == 1
    pubkey_bytes = message_bytes[1:PUBKEY_SIZE + 1]
    if pubkey_bytes not in packer_unpacker_cache.keys():
        packer, unpacker = get_packer_unpacker(redis_connection, pubkey_bytes, local_privkey_bytes=_constants.upstream_privkey_bytes)
        packer_unpacker_cache[pubkey_bytes] = (packer, unpacker)
    else:
        packer, unpacker = packer_unpacker_cache[pubkey_bytes]
    unpacked_message = await unpacker(message_bytes)
    return pubkey_bytes, unpacked_message#, packer_unpacker_cache


async def register_node(request):
    # TODO: generalize this for multiple versions
    connection = request.app['redis']
    posted_bytes = await request.read()
    print(packer_unpacker_cache)
    pubkey_bytes, uid_bytes = await unwrap_message(posted_bytes, connection, packer_unpacker_cache)
    print(packer_unpacker_cache)
    await connection.hset(f'{redis_prefix}_node_pubkey_uid_mapping', pubkey_bytes.hex(), uid_bytes.hex())
    await connection.hset(f'{redis_prefix}_node_earliest_timestamp_pubkey', pubkey_bytes.hex(), time.time())
    msg = status = b'\x01'
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](msg)
    return web.Response(body=encrypted_message)

async def redistribute(loop=None):
    redis_connection = await init_redis("proxy")
    packer_unpacker_cache = {}
    async for udp_reading in pseudosub(redis_connection, 'udp_inbound'):
        udp_bytes = udp_reading.value
        pubkey_bytes, update_msg = await unwrap_message(udp_bytes, redis_connection, packer_unpacker_cache)
        uid = await redis_connection.hget(f'{redis_prefix}_node_pubkey_uid_mapping', pubkey_bytes.hex())
        now = time.time()
        await redis_connection.hset(f'{redis_prefix}_latest_value_frame', uid, cbor.dumps(update_msg))
        await redis_connection.hset(f'{redis_prefix}_latest_message_timestamp', uid, now)
        await pseudopub(redis_connection, [f'updates_{uid.decode()}'], now, update_msg)

async def check_received(request):
    connection = request.app['redis']
    posted_bytes = await request.read()
    pubkey_bytes, uid_bytes = await unwrap_message(posted_bytes, connection, request.app['packers'])
    uid_hex = (await connection.hget(f'{redis_prefix}_node_pubkey_uid_mapping', pubkey_bytes.hex()) or b'').decode()
    assert uid_hex == uid_bytes.hex()
    earliest_timestamp_pubkey = await connection.hget(f'{redis_prefix}_node_earliest_timestamp_pubkey', pubkey_bytes.hex())
    latest_msg_timestamp = await connection.hget(f'{redis_prefix}_latest_message_timestamp', uid_hex)
    earliest_timestamp = float(earliest_timestamp_pubkey or b'0')
    timestamp_latest_msg = float(latest_msg_timestamp or b'0')
    msg = (earliest_timestamp, timestamp_latest_msg)
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](msg)
    return web.Response(body=encrypted_message)

async def start_authenticated_session(request):
    connection = request.app['redis']
    users_db = request.app['users_db']
    posted_bytes = await request.read()
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection, packer_unpacker_cache)
    print(msg)
    if 'signup' in request.rel_url.path:
        signup_required_fields = 'email name nodeSecret passwordHash passwordHint phone'.split()
        for key in signup_required_fields:
            assert msg.get(key)
        user_signin_status, user_info = users_db.check_user(msg.get('email'), msg.get('passwordHash'))
        if user_signin_status == user_datastore.Status.NO_SUCH_USER:
            user_signin_status = users_db.add_user(
                msg.get('name'), msg.get('email'), msg.get('phone'), msg.get('passwordHash'), msg.get('passwordHint'), msg.get('nodeSecret'))
        if user_info is not None:
            serialized_user = dataclasses.asdict(user_info)
        else:
            serialized_user = 'None'
        msg = ('signup', str(user_signin_status), serialized_user)
    if 'login' in request.rel_url.path:
        login_required_fields = ['email', 'passwordHash']
        for key in login_required_fields:
            assert msg.get(key)
        user_signin_status, user_info = users_db.check_user(msg.get('email'), msg.get('passwordHash'))
        if user_info is not None:
            serialized_user = dataclasses.asdict(user_info)
        else:
            serialized_user = 'None'
        msg = ('login', str(user_signin_status), serialized_user)
    if user_signin_status == user_datastore.Status.SUCCESS:
        await connection.hset(f'{redis_prefix}_user_pubkey_uid_mapping', pubkey_bytes.hex(), user_info.node_id)
        await connection.hset(f'{redis_prefix}_user_timestamp_authed_pubkey', pubkey_bytes.hex(), time.time())
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](msg)
    return web.Response(text=base64.b64encode(encrypted_message).decode())


async def get_latest(request):
    connection = request.app['redis']
    posted_bytes = await request.read()
    print(packer_unpacker_cache)
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection, packer_unpacker_cache)
    print(packer_unpacker_cache)
    uid = await connection.hget(f'{redis_prefix}_user_pubkey_uid_mapping', pubkey_bytes.hex())
    latest = await connection.hget(f'{redis_prefix}_latest_value_frame', uid)
    if latest is not None:
        response = cbor.loads(latest)
    else:
        response = 'NO DATA YET'
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](response)
    return web.Response(text=base64.b64encode(encrypted_message).decode())

async def set_alerts(request):
    connection = request.app['redis']
    posted_bytes = await request.read()
    pubkey_bytes, msg = await unwrap_message(posted_bytes, connection, packer_unpacker_cache)
    uid = await connection.hget(f'{redis_prefix}_user_pubkey_uid_mapping', pubkey_bytes.hex())
    if msg:
        await connection.hset(f'{redis_prefix}_uid_alert_mapping', uid, json.dumps(msg))
    else:
        alerts = await connection.hget(f'{redis_prefix}_uid_alert_mapping', uid)
    if alerts is not None:
        response = cbor.loads(alerts)
    else:
        response = 'NO ALERTS YET'
    encrypted_message = await packer_unpacker_cache[pubkey_bytes][0](response)
    return web.Response(text=base64.b64encode(encrypted_message).decode())


def create_app(loop):
    async def start_background_tasks(app):
        app['redis'] = await init_redis("proxy")
        protocol = ProxyDatagramProtocol(loop, await init_redis("proxy"))
        app['udp_task'] = loop.create_task(loop.create_datagram_endpoint(lambda: protocol, local_addr=('0.0.0.0', _constants.upstream_port)))
        app['redistribute_task'] = loop.create_task(redistribute())
        app['users_db'] = user_datastore.UserDatabase(db_name=f'{data_dir}/users_db.db')
        app['packer_unpacker_cache'] = {}

    app = web.Application()
    #app.router.add_get('/ws', init_ws)
    app.router.add_post('/alerts', set_alerts)
    app.router.add_post('/latest', get_latest)
    app.router.add_post('/register', register_node)
    app.router.add_post('/check', check_received)
    app.router.add_post('/signup', start_authenticated_session)
    app.router.add_post('/login', start_authenticated_session)
    app.on_startup.append(start_background_tasks)
    return app


if __name__ == "__main__":
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    redis_server_process = handlebars.start_redis_server(redis_socket_path=data_dir + 'sproutwave.sock')
    handlebars.start_redis_server(redis_socket_path=data_dir + 'proxysproutwave.sock')
    loop = asyncio.get_event_loop()
    app = create_app(loop)
    if _constants.upstream_protocol == 'https':
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=local_dir + "/resources/fullchain.pem", keyfile=local_dir + "/resources/privkey.pem")
        context.set_ciphers('RSA')
    else:
        context = None
    web.run_app(app, host='0.0.0.0', port=_constants.upstream_port, ssl_context=context)
