import asyncio
import time
import gc
import cbor
import numpy as np

import asyncio_redis
from asyncio_redis.encoders import BaseEncoder

import numexpr3 as ne3

import beepshrink

class CborEncoder(BaseEncoder):
    native_type = object
    def encode_from_native(self, data):
        return cbor.dumps(data)
    def decode_to_native(self, data):
        return cbor.loads(data)


async def get_connection():
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder=CborEncoder())
    return redis_connection

async def main():
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()

    print('Configuring SDR...')
    # multiple of 2, 5 - should have neon butterflies for at least these
    # lowest "sane" sample rate
    sdr.rs = 256000
    # TODO: dither the tuning frequency to avoid trampling via LF beating or DC spur
    # offset center frequency for 433.92 ism
    sdr.fc = 434e6
    # arbitrary small number, adjust based on antenna and range
    sdr.gain = 3
    #sdr.set_manual_gain_enabled(False)
    print('  sample rate: %0.3f MHz' % (sdr.rs/1e6))
    print('  center frequency %0.6f MHz' % (sdr.fc/1e6))
    # TODO: if you don't set gain, and query this parameter, python segfaults..
    print('  gain: %s dB' % sdr.gain)

    print('Streaming bytes...')
    await process_samples(sdr)#, redis_connection)
    await sdr.stop()
    print('Done')
    sdr.close()


complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j

def packed_bytes_to_iq(samples, out = None):
    if out is None:
        iq = np.empty(len(samples)//2, 'complex64')
    else:
        iq = out
    bytes_np = np.ctypeslib.as_array(samples)
    iq.real, iq.imag = bytes_np[::2], bytes_np[1::2]
    iq /= (255/2)
    iq -= (1 + 1j)
    if out is None:
        return iq

async def push_sample(timestamp, info):
    connection = await get_connection()
    await connection.set(timestamp, info)
    await connection.lpush('eof_timestamps', [timestamp])
    await connection.expireat(timestamp, int(timestamp+600))

async def process_samples(sdr):
    (block_size, max_blocks) = (1024*32, 40)
    samp_size = block_size // 2
    (total, acc, count) = (0, 0, 1)
    last = time.time()
    loud = False
    complex_samples = np.empty(samp_size, 'complex64')
    float_samples = np.empty(samp_size, dtype='float32')
    block = np.empty(samp_size*max_blocks, 'complex64')
    absoluter = ne3.NumExpr('float_samples = abs(complex_samples)')
    async for byte_samples in sdr.stream(block_size, format='bytes'):
        packed_bytes_to_iq(byte_samples, complex_samples)
        #ne3.evaluate('float_samples = abs(complex_samples)')
        absoluter.run(float_samples = float_samples, complex_samples = complex_samples)
        #float_samples = np.abs(complex_samples)
        pwr = np.sum(float_samples)
        if total == 0:
            total = pwr
        if pwr > (total / count):
            total += (pwr - (total/count))/100.
            print(time.time()-last, acc, pwr, total/count)
            loud = True
        if loud == True:
            acc += 1
            if acc < max_blocks:
                block[samp_size*acc:samp_size*(acc+1)] = complex_samples
        if pwr < (total / count):
            total += pwr
            count += 1
            if loud:
                timestamp = time.time()
                size, dtype, compressed = beepshrink.compress(block[0:(acc+1)*samp_size])
                info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                await push_sample(timestamp, info)
                block.fill(0+0j)
                print(time.time()-last, acc, pwr, total/count)
                loud = False
                acc = 0
        last = time.time()
        if (count % 1000) == 0:
            gc.collect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
