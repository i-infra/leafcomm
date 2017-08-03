import numpy as np
import numexpr3 as ne3
import bottleneck as bn
import itertools as its

import statistics
import typing
import multiprocessing
import asyncio
import time
import gc
import random
import zlib

import cbor
import blosc
import asyncio_redis

from asyncio_redis.encoders import BaseEncoder, BytesEncoder

import tsd

FilterFunc = typing.Callable[[np.ndarray], np.ndarray]
Info = typing.Dict
Deciles = typing.Dict[int,typing.Tuple[int, int]]
Packet = typing.NamedTuple('Packet', [('packet', list), ('errors', list), ('deciles', dict), ('raw', list)])

complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j
rerle = lambda xs: [(sum([i[0] for i in x[1]]), x[0]) for x in its.groupby(xs, lambda x: x[1])]
rle = lambda xs: [(len(list(gp)), x) for x, gp in its.groupby(xs)]
rld = lambda xs: its.chain.from_iterable(its.repeat(x, n) for n, x in xs)

(L,H,E) = (0,1,2)

printer = lambda xs: ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])
debinary = lambda ba: sum([x*(2**i) for (i,x) in enumerate(reversed(ba))])
brickwall = lambda xs: bn.move_mean(xs, 32, 1)

try:
    import scipy
    from scipy.signal import butter, lfilter, freqz
    def butter_filter(data, cutoff, fs, btype = 'low', order = 5):
        normalized_cutoff = cutoff / (0.5*fs)
        if btype == 'bandpass':
            normalized_cutoff = [normalized_cutoff // 10, normalized_cutoff]
        b, a = butter(order, normalized_cutoff, btype = btype, analog = False)
        b = b.astype('float32')
        a = a.astype('float32')
        y = lfilter(b, a, data)
        return y
    lowpass = lambda xs: butter_filter(xs, 2e3, 256e3, 'low', order = 4)
    #bandpass = lambda xs: butter_filter(xs, 2e3, 256e3, 'bandpass', order = 4)
    filters = [brickwall, lowpass]
except:
    filters = [brickwall]


class CborEncoder(BaseEncoder):
    native_type = object
    def encode_from_native(self, data):
        return cbor.dumps(data)
    def decode_to_native(self, data):
        return cbor.loads(data)

compress = lambda in_: (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel = 1, shuffle = blosc.BITSHUFFLE, cname = 'lz4'))

def decompress(size: int, dtype: str, data: bytes) -> np.ndarray:
    out = np.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__['data'][0])
    return out

def packed_bytes_to_iq(samples: bytes, out = None) -> typing.Union[None, np.ndarray]:
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

async def process_samples(sdr) -> typing.Awaitable[None]:
    connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    (block_size, max_blocks) = (1024*32, 40)
    samp_size = block_size // 2
    (total, acc, count) = (0, 0, 1)
    last = time.time()
    loud = False
    block = []
    async for byte_samples in sdr.stream(block_size, format = 'bytes'):
        complex_samples = np.empty(samp_size, 'complex64')
        packed_bytes_to_iq(byte_samples, complex_samples)
        pwr = np.sum(np.abs(complex_samples))
        if ((count % 1000) == 1) and (loud is False):
            sdr.set_center_freq(random.randrange(int(433.8e6), int(434e6), step = 10000))
            print("center frequency:", sdr.get_center_freq())
        if total == 0:
            total = pwr
        if pwr > (total / count):
            total += (pwr - (total/count))/100.
            loud = True
            block.append(np.copy(complex_samples))
            acc += 1
        else:
            total += pwr
            count += 1
            if loud is True:
                timestamp = time.time()
                block.append(np.copy(complex_samples))
                size, dtype, compressed = compress(np.concatenate(block))
                info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                await connection.set(timestamp, info)
                await connection.lpush('eof_timestamps', [timestamp])
                await connection.expireat(timestamp, int(timestamp+600))
                print('flushing:', time.time()-last, acc, pwr, total/count, count)
                block = []
                loud = False
                acc = 0
                gc.collect()
        last = time.time()
    return None

def get_pulses_from_info(info: Info, smoother: FilterFunc = brickwall) -> np.ndarray:
    beep_samples = decompress(**info)
    shape = beep_samples.shape
    beep_absolute = np.empty(shape, dtype = 'float32')
    ne3.evaluate('beep_absolute = abs(beep_samples)')
    beep_smoothed = smoother(beep_absolute)
    threshold = 1.1*bn.nanmean(beep_smoothed)
    beep_binary = np.empty(shape, dtype=bool)
    ne3.evaluate('beep_binary = beep_smoothed > threshold')
    pulses = rle(beep_binary)
    return np.array(pulses)

def get_decile_durations(pulses: np.ndarray) -> Deciles:
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

def find_pulse_groups(pulses: np.ndarray, deciles: Deciles) -> typing.List[int]:
    cutoff = min(deciles[0][0],deciles[1][0])*9
    breaks = np.logical_and(pulses.T[0] > cutoff, pulses.T[1] == L).nonzero()[0]
    break_deltas = np.diff(breaks)
    if (len(breaks) > 50) or (len(breaks) < 5):
        return []
    elif len(np.unique(break_deltas[np.where(break_deltas > 3)])) > 3:
        try:
            d_mode = statistics.mode([bd for bd in break_deltas if bd > 3])
        except statistics.StatisticsError:
            d_mode = round(statistics.mean(break_deltas))
        expected_breaks = [x*d_mode for x in range(round(max(breaks) // d_mode))]
        if len(expected_breaks) < 2:
            return []
        tolerance = d_mode//10
        breaks = [x for (x,bd) in zip(breaks, break_deltas)
            if (True in [abs(x-y) < tolerance for y in expected_breaks]) or (bd == d_mode) or (bd < 5)]
    return breaks

def demodulator(pulses: np.ndarray) -> typing.Iterable[Packet]:
    deciles = get_decile_durations(pulses)
    if deciles == {}:
        return []
    breaks = find_pulse_groups(pulses, deciles)
    if len(breaks) == 0:
        return []
    for (x,y) in zip(breaks, breaks[1::]):
        pulse_group = pulses[x+1:y]
        bits = []
        errors = []
        for chip in pulse_group:
            valid = False
            for v in deciles.keys():
                for (i, width) in enumerate(deciles[v]):
                    if (not valid) and (chip[1] == v) and (abs(chip[0] - width) < width // 2):
                        bits += [v]*(i+1)
                        valid = True
            if not valid:
                errors += [chip]
                bits.append(E)
        if len(bits) > 4:
            result = Packet(bits, errors, deciles, pulses[x:y])
            yield result

def silver_sensor(packet: Packet) -> typing.Dict:
    # TODO: CRC
    # TODO: battery OK
    # TODO: handle preamble pulse
    if packet.errors == []:
        bits = [x[0] == 2 for x in rle(packet.packet) if x[1] == 0]
        # some thanks to http://forum.iobroker.net/viewtopic.php?t=3818
        # "TTTT=Binär in Dez., Dez als HEX, HEX in Dez umwandeln (zB 0010=2Dez, 2Dez=2 Hex) 0010=2 1001=9 0110=6 => 692 HEX = 1682 Dez = >1+6= 7 UND 82 = 782°F"
        if len(bits) == 42:
            fields = [0,2,8,2,2,4,4,4,4,4,8]
            fields = [x for x in its.accumulate(fields)]
            results = [debinary(bits[x:y]) for (x,y) in zip(fields, fields[1:])]
            if results[1] == 255:
                return {}
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
            fields = [x for x in its.accumulate(fields)]
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
    return {}


def decode_pulses(pulses: np.ndarray) -> typing.Dict:
    if len(pulses) > 10:
        for packet in demodulator(pulses):
            res = silver_sensor(packet)
            if 'uid' in res.keys():
                print(printer(packet.packet))
                res['uid'] = (res['channel']+1*1024)+res['uid']
                return res
    return {}

def try_decode(info: typing.Dict, timestamp = 0) -> typing.Dict:
    for filter_func in filters:
        pulses = get_pulses_from_info(info, filter_func)
        decoded = decode_pulses(pulses)
        if decoded != {}:
            return decoded
    return {}

async def phase1_main() -> typing.Awaitable[None]:
    print('initialising SDR...')
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()

    sdr.rs, sdr.fc, sdr.gain = 256000, 433.9e6, 3

    print('streaming bytes...')
    await process_samples(sdr)
    await sdr.stop()
    print('Done')
    sdr.close()
    return None

async def packetizer_main() -> typing.Awaitable[None]:
    connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    while True:
        try:
            timestamp = await connection.brpop(['eof_timestamps'], 360)
            timestamp = timestamp.value
            info = await connection.get(timestamp)
        except:
            info = {}
        if info in [{}, None]:
            decoded = {}
        else:
            decoded = try_decode(info, timestamp)
        if decoded == {}:
            await connection.expire(timestamp, 3600)
            await connection.sadd('nontrivial_timestamps', [timestamp])
        else:
            await connection.publish('sensor_readings', [timestamp, decoded['uid'], tsd.degc, float(decoded['temperature'])])
            await connection.publish('sensor_readings', [timestamp, decoded['uid'], tsd.rh, float(decoded['humidity'])])
            await connection.sadd('trivial_timestamps', [timestamp])
            await connection.delete([timestamp])
    return None

async def logger_main() -> typing.Awaitable[None]:
    connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    datastore = tsd.TimeSeriesDatastore()
    print('connected to datastore')
    subscription = await connection.start_subscribe()
    await subscription.subscribe(['sensor_readings'])
    while True:
        reading = await subscription.next_published()
        datastore.add_measurement(*(reading.value), raw=True)
    return None


update_interval = 120

async def relay_main() -> typing.Awaitable[None]:
    connection = await asyncio_redis.Connection.create('localhost', 6379, encoder = CborEncoder())
    subscription = await connection.start_subscribe()
    await subscription.subscribe(['sensor_readings'])
    samples = {}
    last_sent = time.time()
    while True:
        reading_bytes = await subscription.next_published()
        reading = reading_bytes.value
        samples[reading[1]+4096*reading[2]] = reading
        if time.time() > (last_sent + update_interval):
            update_samples = cbor.dumps([x for x in samples.values()])
            print(len(update_samples), len(zlib.compress(update_samples)), update_samples)
            last_sent = time.time()
            samples = {}
    return None

def spawner(future_yielder):
    def loopwrapper(main):
        loop = asyncio.get_event_loop()
        asyncio.ensure_future(main())
        loop.run_forever()
    multiprocessing.Process(target = loopwrapper, args = (future_yielder,)).start()

if __name__ == "__main__":
    spawner(phase1_main)
    asyncio.ensure_future(packetizer_main())
    asyncio.ensure_future(logger_main())
    asyncio.ensure_future(relay_main())
    asyncio.get_event_loop().run_forever()
