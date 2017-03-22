import asyncio
import time
import gc
import cbor
import blosc
import numpy as np

import asyncio_redis
from asyncio_redis.encoders import BaseEncoder

import numexpr3 as ne3

class CborEncoder(BaseEncoder):
    native_type = object
    def encode_from_native(self, data):
        return cbor.dumps(data)
    def decode_to_native(self, data):
        return cbor.loads(data)

compress = lambda in_: (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))

def decompress(size, dtype, data):
    out = np.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__['data'][0])
    return out

async def get_connection():
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder=CborEncoder())
    return redis_connection

async def main():
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()

    print('Configuring SDR...')
    sdr.rs = 256000
    # TODO: dither the tuning frequency to avoid trampling via LF beating or DC spur
    sdr.fc = 433.8e6
    sdr.gain = 3
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

async def push_sample(connection, timestamp, info):
    await connection.set(timestamp, info)
    await connection.lpush('eof_timestamps', [timestamp])
    await connection.expireat(timestamp, int(timestamp+600))

async def process_samples(sdr):
    connection = await get_connection()
    (block_size, max_blocks) = (1024*32, 40)
    samp_size = block_size // 2
    (total, acc, count) = (0, 0, 1)
    last = time.time()
    loud = False
    block = []
    async for byte_samples in sdr.stream(block_size, format='bytes'):
        complex_samples = np.empty(samp_size, 'complex64')
        packed_bytes_to_iq(byte_samples, complex_samples)
        #float_samples = np.empty(samp_size, 'float32')
        #ne3.evaluate('float_samples = abs(complex_samples)')
        pwr = np.sum(np.abs(complex_samples))
        if total == 0:
            total = pwr
        if pwr > (total / count):
            total += (pwr - (total/count))/100.
            print(time.time()-last, acc, pwr, total/count)
            loud = True
            block.append(np.copy(complex_samples))
            acc += 1
        else:
            total += pwr
            count += 1
            if loud:
                timestamp = time.time()
                block.append(np.copy(complex_samples))
                #size, dtype, compressed = beepshrink.compress(block[0:(acc+1)*samp_size])
                size, dtype, compressed = compress(np.concatenate(block))
                info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                await push_sample(connection, timestamp, info)
                print(time.time()-last, acc, pwr, total/count, count)
                block = []
                loud = False
                acc = 0
                gc.collect()
        last = time.time()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
