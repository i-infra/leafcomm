import asyncio
import uuid
import base64
import logging
import zlib
import time
import socket

import urllib.request
import glob
import cbor
import nacl
import nacl.public, nacl.hash, nacl.encoding
import asyncio_redis
from asyncio_redis.encoders import BaseEncoder

logging.getLogger().setLevel(logging.DEBUG)
relay_key = nacl.public.PublicKey(b'D\x8e\x9cT\x8b\xec\xb7\xf4\x17\xea\xa6\x8c\x11\xd3U\xb0\xbc\xe0\xb32\x15t\xbb\xe49^Y\xbf2\x8dUo')

guid = lambda : base64.urlsafe_b64encode(uuid.uuid4().bytes).decode('utf-8')

def get_hardware_uid():
    import nacl.hash, nacl.encoding
    hashed = nacl.hash.sha512(open(glob.glob("/sys/block/mmcblk*/device/cid")[0], 'rb').read(), encoder=nacl.encoding.RawEncoder)
    colour = open('uid/colours').read().split()[hashed[-1]>>4]
    veggy = open('uid/veggies').read().split()[hashed[-1]&0xF]
    human_name = "%s %s" % (colour, veggy)
    return human_name, hashed 

def register_session_box():
    import nacl.public
    human_name, uid = get_hardware_uid() 
    session_key = nacl.public.PrivateKey.generate()
    identity_pair = [uid, session_key.public_key.encode()]
    signed_message = nacl.public.SealedBox(relay_key).encrypt(cbor.dumps(identity_pair))
    response = urllib.request.urlopen('https://data.sproutwave.com:8444/register', data=signed_message)
    assert response.getcode() == 200
    return uid, nacl.public.Box(session_key, relay_key)

update_interval = 2

async def start_udp_client(loop, host, port, box):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    logging.debug('udp relayer connected to redis')
    samples = {}
    last_sent = time.time()
    async for reading in metasub(connection, 'outbound_channel'):
        reading = await asyncio.wait_for(subscription.next_published(), update_interval)
        if reading is not None:
            samples[reading[1]+4096*reading[2]] = reading
        if time.time() > (last_sent + update_interval):
            update_samples = [x for x in samples.values()]
            logging.debug(update_samples)
            sock.sendto(box.encrypt(zlib.compress(cbor.dumps(update_samples))), (host, port))
            last_sent = time.time()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    uid, session_box = register_session_box()
    loop.create_task(start_udp_client(loop, 'data.sproutwave.com', 8019, session_box))
    loop.run_forever()
