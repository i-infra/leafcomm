import asyncio
import time
import gc
import itertools
import typing
import cbor
import blosc

import numpy as np

import numexpr3 as ne3
import bottleneck as bn

from asyncio_redis.encoders import BaseEncoder
from statistics import mode, mean, StatisticsError

ne3.set_num_threads(2)

compress = lambda in_: (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))

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
rle = lambda xs: [(len(list(gp)), x) for x, gp in itertools.groupby(xs)]
rld = lambda xs: itertools.chain.from_iterable(itertools.repeat(x, n) for n, x in xs)

(L,H,E) = (0,1,2)

printer = lambda xs: ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])
debinary = lambda ba: sum([x*(2**i) for (i,x) in enumerate(reversed(ba))])
smoother = lambda xs: bn.move_mean(xs, 32, 1)


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

async def phase1_main():
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


def get_pulses_from_info(info):
    beep_samples = decompress(**info)
    shape = beep_samples.shape
    beep_absolute = np.empty(shape, dtype='float32')
    ne3.evaluate('beep_absolute = abs(beep_samples)')
    beep_smoothed = smoother(beep_absolute)
    threshold = 0.5*bn.nanmax(beep_smoothed)
    beep_binary = np.empty(shape, dtype=bool)
    ne3.evaluate('beep_binary = beep_smoothed > threshold')
    pulses = np.array(rle(beep_binary))
    return pulses

def get_decile_durations(pulses): # -> { 1: (100, 150), 0: (200, 250) }
    values = np.unique(pulses.T[1])
    deciles = {}
    if len(pulses) < 10:
        return {}
    for value in values:
        counts = np.sort(pulses[np.where(pulses.T[1] == value)].T[0])
        tenth = len(counts) // 10
        if not tenth:
            return {}
        short_decile = int(np.mean(counts[1*tenth:2*tenth]))
        long_decile = int(np.mean(counts[8*tenth:9*tenth]))
        deciles[value] = (short_decile, long_decile)
    return deciles

def find_pulse_groups(pulses, deciles): # -> [0, 1111, 1611, 2111]
    cutoff = min(deciles[0][0],deciles[1][0])*9
    breaks = np.where(pulses.T[0] > cutoff)[0]
    break_deltas = np.diff(breaks)
    if len(break_deltas) < 2:
        return []
    elif len(np.unique(break_deltas[np.where(break_deltas > 3)])) > 3:
        try:
            d_mode = mode(break_deltas)
        # if all values different, use mean as mode
        except StatisticsError:
            d_mode = round(mean(break_deltas))
        # determine expected periodicity of packet widths
        breaks2 = [x*d_mode for x in range(round(max(breaks) // d_mode))]
        if len(breaks2) < 2:
            return []
        # discard breaks more than 10% from expected position
        breaks = [x for x in breaks if True in [abs(x-y) < breaks2[1]//10 for y in breaks2]]
    return breaks

PacketBase = typing.NamedTuple('PacketBase', [('packet', list), ('errors', list), ('deciles', dict), ('raw', list)])

def demodulator(pulses): # -> generator<PacketBase>
    deciles = get_decile_durations(pulses)
    if deciles == {}:
        return []
    breaks = find_pulse_groups(pulses, deciles)
    if len(breaks) == 0:
        return []
    for (x,y) in zip(breaks, breaks[1::]):
        packet = pulses[x+1:y]
        pb = []
        errors = []
        # iterate over packet pulses
        for chip in packet:
            valid = False
            for v in deciles.keys():
                for (i, width) in enumerate(deciles[v]):
                    if (not valid) and (chip[1] == v) and (abs(chip[0] - width) < width // 2):
                        pb += [v]*(i+1)
                        valid = True
            if not valid:
                errors += [chip]
                pb.append(E)
        if len(pb) > 4:
            result = PacketBase(pb, errors, deciles, pulses[x:y])
            yield result

def silver_sensor(packet): # -> None | dictionary
    # TODO: CRC
    # TODO: battery OK
    # TODO: handle preamble pulse
    if packet.errors == []:
        bits = [x[0] == 2 for x in rle(packet.packet) if x[1] == 0]
        # some thanks to http://forum.iobroker.net/viewtopic.php?t=3818
        # "TTTT=Binär in Dez., Dez als HEX, HEX in Dez umwandeln (zB 0010=2Dez, 2Dez=2 Hex) 0010=2 1001=9 0110=6 => 692 HEX = 1682 Dez = >1+6= 7 UND 82 = 782°F"
        if len(bits) == 42:
            fields = [0,2,8,2,2,4,4,4,4,4,8]
            fields = [x for x in itertools.accumulate(fields)]
            results = [debinary(bits[x:y]) for (x,y) in zip(fields, fields[1:])]
            # uid is never 0xff, but similar protocols sometimes decode with this field as 0xFF
            if results[1] == 255:
                return None
            temp = (16**2*results[6]+16*results[5]+results[4])
            humidity = (16*results[8]+results[7])
            if temp > 1000:
                temp %= 1000
                temp += 100
            temp /= 10
            temp -= 32
            temp *= 5/9
            return {'uid':results[1], 'temperature': temp, 'humidity': humidity, 'channel':results[3]}#, 'metameta': packet.__dict__}
        elif len(bits) == 36:
            fields = [0] + [4]*9
            fields = [x for x in itertools.accumulate(fields)]
            n = [debinary(bits[x:y]) for (x,y) in zip(fields, fields[1:])]
            model = n[0]
            uid = n[1] << 4 | n[2]
            temp = n[4] << 8 | n[5] << 4 | n[6]
            if temp >= 0xf00:
                temp -= 0x1000
            channel = n[3]&0x3
            battery_ok = (0b1000 & n[2]) == 0
            temp /= 10
            rh = n[7]*10 + n[8]
            if n[0] == 5:
                return {'uid': uid, 'temperature': temp, 'humidity': rh, 'channel': channel}
    return None

async def packetizer_main():
    import tsd
    connection = await get_connection()
    datastore = tsd.TimeSeriesDatastore()
    while True:
        timestamp = await connection.brpop(['eof_timestamps'], 360)
        timestamp = timestamp.value
        info = await connection.get(timestamp)
        if info in [{}, None]:
            pulses = []
        else:
            pulses = get_pulses_from_info(info)
        decoded = False
        if len(pulses) > 10:
            for packet in demodulator(pulses):
                print(printer(packet.packet))
                res = silver_sensor(packet)
                if (res is not None) and decoded is False:
                    uid = (res['channel']+1*1024)+res['uid']
                    datastore.add_measurement(timestamp, uid, 'degc', res['temperature'])
                    datastore.add_measurement(timestamp, uid, 'rh', res['humidity'])
                    print(res)
                    decoded = True
                    break
        if (len(pulses) != 0) and (decoded == False):
            await connection.expire(timestamp, 3600)
            await connection.sadd('nontrivial_timestamps', [timestamp])
        else:
            await connection.delete([timestamp])

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(packetizer_main())
    loop.run_until_complete(phase1_main())
