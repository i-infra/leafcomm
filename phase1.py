import asyncio
import math
import time
import numpy as np
import beepshrink

import asyncio_redis
from asyncio_redis.encoders import BytesEncoder, BaseEncoder 
import cbor


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
    redis_connection = await get_connection() 
    await process_samples(sdr, redis_connection)
    await sdr.stop()

    print('Done')

    sdr.close()


complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j

async def process_samples(sdr, connection):
    async def packed_bytes_to_iq(samples):
        return sdr.packed_bytes_to_iq(samples)

    total = 0
    count = 1
    last = time.time()
    relevant_blocks = []
    block_time = time.time()
    loud = False
    async for samples in sdr.stream(1024*32, format='bytes'):
        floats = await packed_bytes_to_iq(samples)
        pwr = np.sum(np.absolute(floats))
        if total == 0:
            total = pwr
        if pwr > (total / count):
            total += (pwr - (total/count))/100.
            print(time.time()-last, len(samples)/sdr.rs, pwr, total/count)
            loud = True
            relevant_blocks.append(floats)
        else:
            total += pwr
            count += 1
            if loud:
                relevant_blocks.append(floats)
                timestamp = time.time()
                block = np.concatenate(relevant_blocks)
                size, dtype, compressed = beepshrink.compress(block)
                frame_samples = {'size': size, 'dtype': dtype.name, 'data': compressed}
                await connection.set(timestamp, frame_samples) 
                await connection.lpush('eof_timestamps', [timestamp])
                await connection.expireat(timestamp, int(timestamp+600))
                print(len(block)/sdr.rs, pwr, total/count, size, dtype, len(compressed) / block.nbytes)
                loud = False
                relevant_blocks = []
        last = time.time()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
