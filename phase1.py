import asyncio
from rtlsdr import rtlsdraio
import math
import time
import numpy as np
import beepshrink

import asyncio_redis
from asyncio_redis.encoders import BytesEncoder, BaseEncoder 

import cbor


# TODO: Fix this, it throws several varieties of errors when used in conjunction with pub-sub
class CborEncoder(BaseEncoder):
    native_type = dict
    def encode_from_native(self, data):
        cbor.dumps(data)
    def decode_to_native(self, data):
        cbor.loads(data)

async def main():
    sdr = rtlsdraio.RtlSdrAio()

    print('Configuring SDR...')
    # multiple of 2, 5 - should have neon butterflies for at least these
    # lowest "sane" sample rate
    sdr.rs = 256000
    # center frequency for 433.92 ism
    sdr.fc = 433.9e6
    # arbitrary small number, adjust based on antenna and range
    sdr.gain = 3
    #sdr.set_manual_gain_enabled(False)
    print('  sample rate: %0.3f MHz' % (sdr.rs/1e6))
    print('  center frequency %0.6f MHz' % (sdr.fc/1e6))
    # TODO: if you don't set gain, and query this parameter, python segfaults..
    print('  gain: %s dB' % sdr.gain)


    print('Streaming bytes...')
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder=BytesEncoder())
    async def enqueuer(data):
        msg = cbor.dumps(data)
        await redis_connection.publish(b'beep-blobs', msg)
    await process_samples(sdr, enqueuer)
    await sdr.stop()

    print('Done')

    sdr.close()


complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j

async def get_dequeuer():
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder=BytesEncoder())
    subscriber = await redis_connection.start_subscribe()
    await subscriber.subscribe([b'beep-blobs'])
    return subscriber

async def process_samples(sdr, enqueuer):
    async def packed_bytes_to_iq(samples):
        return sdr.packed_bytes_to_iq(samples)

    total = 0
    count = 0
    last = time.time()
    relevant_blocks = []
    block_time = time.time()
    loud = False
    async for samples in sdr.stream(1024*32, format='bytes'):
        floats = await packed_bytes_to_iq(samples)
        pwr = np.sum(np.absolute(floats))
        total += pwr
        count += 1
        if pwr > (total / count):
            print(time.time()-last, len(samples)/sdr.rs, pwr, total/count)
            loud = True
            relevant_blocks.append(floats)
        else:
            if loud:
                block = np.concatenate(relevant_blocks)
                size, dtype, compressed = beepshrink.compress(block)
                await enqueuer({'time': time.time(), 'size': size, 'dtype': dtype.name, 'data': compressed})
                print(len(block)/sdr.rs, pwr, total/count, size, dtype, len(compressed) / block.nbytes)
                loud = False
                relevant_blocks = []
        last = time.time()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
