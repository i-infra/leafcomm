from aiohttp import web
import asyncio
import uuid
import base64
import logging

import aiohttp
import requests

import cbor
import nacl
import nacl.public
import asyncio_redis

import phase1

relay_key = nacl.public.PublicKey(b'D\x8e\x9cT\x8b\xec\xb7\xf4\x17\xea\xa6\x8c\x11\xd3U\xb0\xbc\xe0\xb32\x15t\xbb\xe49^Y\xbf2\x8dUo')

guid = lambda : base64.urlsafe_b64encode(uuid.uuid4().bytes).decode('utf-8')

def register_box():
    uid = guid()
    node_key = nacl.public.PrivateKey.generate()
    identity_pair = [uid, node_key.public_key.encode()]
    signed_message = nacl.public.SealedBox(relay_key).encrypt(cbor.dumps(identity_pair))
    response = requests.post('http://localhost:8019/register', signed_message)
    assert response.status_code == 200
    return uid, nacl.public.Box(node_key, relay_key)

import aioudp

async def start_udp_client(loop, host, port, box):
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    pubsub_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = phase1.CborEncoder())
    subscription = await pubsub_connection.start_subscribe()
    await subscription.subscribe(['sensor_readings'])
    logging.debug('udp relayer connected to redis')
    udp = await aioudp.open_remote_endpoint(host=host, port=port, loop=loop)
    while True:
        reading_bytes = await subscription.next_published()
        udp.write(box.encrypt(cbor.dumps(reading_bytes.value)))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    uid, box = register_box()
    print(uid)
    input('send_msg?')
    loop.create_task(start_udp_client(loop, 'localhost', 8019, box))
    loop.run_forever()
