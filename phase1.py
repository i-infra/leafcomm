import asyncio
import time
import gc
import cbor
import blosc

import numpy as np

from asyncio_redis.encoders import BaseEncoder

import numexpr3 as ne3

compress = lambda in_: blosc.compress(in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))

def decompress(size, dtype, data):
    out = np.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__['data'][0])
    return out

class CborEncoder(BaseEncoder):
    native_type = object
    def encode_from_native(self, data):
        return cbor.dumps(data)
    def decode_to_native(self, data):
        return cbor.loads(data)


async def get_connection():
    import asyncio_redis
    redis_connection = await asyncio_redis.Connection.create('localhost', 6379, encoder=CborEncoder())
    return redis_connection

complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j

def packed_bytes_to_iq(samples):
    bytes_np = np.ctypeslib.as_array(samples)
    iq = np.empty(len(samples)//2, 'complex64')
    iq.real, iq.imag = bytes_np[::2], bytes_np[1::2]
    iq /= (255/2)
    iq -= (1 + 1j)
    return iq

async def process_samples(sdr, connection):
    block_size = 1024*32
    total = 0
    count = 1
    last = time.time()
    relevant_blocks = []
    block_time = time.time()
    loud = False
    float_samples = np.empty(block_size//2, dtype='float32')
    async for byte_samples in sdr.stream(block_size, format='bytes'):
        complex_samples = packed_bytes_to_iq(byte_samples)
        abs_expr = ne3.NumExpr('float_samples = abs(complex_samples)')
        abs_expr.run(check_arrays = False)
        pwr = np.sum(float_samples)
        if total == 0:
            total = pwr
        if pwr > (total / count):
            total += (pwr - (total/count))/100.
            print(time.time()-last, (block_size/2)/sdr.rs, pwr, total/count)
            loud = True
            relevant_blocks.append(complex_samples)
        else:
            total += pwr
            count += 1
            if loud:
                relevant_blocks.append(complex_samples)
                timestamp = time.time()
                block = np.concatenate(relevant_blocks)
                size, dtype, compressed = compress(block)
                info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                await connection.set(timestamp, info)
                await connection.lpush('eof_timestamps', [timestamp])
                await connection.expireat(timestamp, int(timestamp+600))
                print(len(block)/sdr.rs, pwr, total/count, size, dtype, len(compressed) / block.nbytes)
                loud = False
                relevant_blocks = []
                gc.collect()
        last = time.time()

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
    redis_connection = await get_connection()
    print('Connected to Redis...')
    await process_samples(sdr, redis_connection)
    await sdr.stop()
    print('Done')
    sdr.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
