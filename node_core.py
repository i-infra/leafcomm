import os
import sys
import statistics
import typing
import multiprocessing
import subprocess
import asyncio
import time
import logging
import base64
import itertools as its

import numpy as np
import bottleneck as bn

import cbor
import blosc
import asyncio_redis

from asyncio_redis.encoders import BaseEncoder, BytesEncoder

import ts_datastore as tsd

logging.getLogger().setLevel(logging.INFO)

FilterFunc = typing.Callable[[np.ndarray], np.ndarray]
Info = typing.Dict
Deciles = typing.Dict[int,typing.Tuple[int, int]]
Packet = typing.NamedTuple('Packet', [('packet', list), ('errors', list), ('deciles', dict), ('raw', list)])

complex_to_stereo = lambda a: np.dstack((a.real, a.imag))[0]
stereo_to_complex = lambda a: a[0] + a[1]*1j
rerle = lambda xs: [(sum([i[0] for i in x[1]]), x[0]) for x in its.groupby(xs, lambda x: x[1])]
rle = lambda xs: [(len(list(gp)), x) for x, gp in its.groupby(xs)]
rld = lambda xs: its.chain.from_iterable(its.repeat(x, n) for n, x in xs)
get_function_name = lambda depth=0: sys._getframe(depth + 1).f_code.co_name
(L,H,E) = (0,1,2)

printer = lambda xs: ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])
debinary = lambda ba: sum([x*(2**i) for (i,x) in enumerate(reversed(ba))])
brickwall = lambda xs: bn.move_mean(xs, 32, 1)

local_dir = os.path.dirname(os.path.realpath(__file__))

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
    else:
        return True

async def init_redis():
    connection = await asyncio_redis.Connection.create('localhost', 6380, encoder = CborEncoder())
    return connection

async def tick(connection, function_name_depth=1, key = 'sproutwave_ticks'):
    name = get_function_name(function_name_depth)
    await connection.hset(key, name, time.time())

async def get_ticks(connection = None, key = 'sproutwave_ticks'):
    if connection == None:
        connection = await init_redis()
    return await connection.hgetall_asdict(key)

async def pseudopub(connection, channels, timestamp, info):
    if isinstance(channels, str):
        channels = [channels]
    if timestamp == None:
        timestamp = time.time()
    await connection.set(timestamp, info)
    for channel in channels:
        await connection.lpush(channel, [timestamp])
    await connection.expireat(timestamp, int(timestamp+600))

async def pseudosub(connection, channel, timeout=360):
    while True:
        yield await pseudosub1(connection, channel, timeout)

async def pseudosub1(connection, channel, timeout=360, depth=3, do_tick=True):
    try:
        timestamp = await connection.brpop([channel], timeout)
        timestamp = timestamp.value
        info = await connection.get(timestamp)
    except:
        timestamp = info = None
    if do_tick == True:
        await tick(connection, function_name_depth=depth)
    return (timestamp, info)

async def analog_to_block() -> typing.Awaitable[None]:
    import gc
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()
    sdr.rs, sdr.fc, sdr.gain = 256000, 433.9e6, 9
    connection = await init_redis()
    block_size  = 512*64
    samp_size = block_size // 2
    (loud, blocks, total, count) = (False, [], 0,  1)
    center_frequency = 433.9e6
    get_new_center = lambda: int(433.8e6) + (int(time.time()*10)%10)*20000
    async for byte_samples in sdr.stream(block_size, format = 'bytes'):
        await tick(connection)
        complex_samples = np.empty(samp_size, 'complex64')
        packed_bytes_to_iq(byte_samples, complex_samples)
        pwr = np.sum(np.abs(complex_samples))
        avg = total/count
        if ((count % 1000) == 1) and (loud is False):
            if count > 10000:
                (total, count) = (avg, 1)
            center_frequency = get_new_center()
            sdr.set_center_freq(center_frequency)
            logging.info("center frequency: %d" % (sdr.get_center_freq()))
        if total == 0:
            total = pwr
        if pwr > avg:
            if loud is False:
                timestamp = time.time()
            total += (pwr - avg)/100.
            loud = True
            blocks.append(np.copy(complex_samples))
        else:
            total += pwr
            count += 1
            if loud is True:
                if len(blocks) > 9:
                    blocks.append(np.copy(complex_samples))
                    block = np.concatenate(blocks)
                    size, dtype, compressed = compress(block)
                    info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                    await pseudopub(connection, 'eof_timestamps', timestamp, info)
                    logging.info('flushing: %s' % {'duration': time.time()-timestamp, 'center_freq': center_frequency, 'block_count': len(blocks), 'block_power': np.sum(np.abs(block))/len(blocks), 'avg': avg, 'lifetime_blocks': count})
                    now = await connection.hget('good_block', center_frequency)
                    if now == None:
                        now = 0
                    await connection.hset('good_block', center_frequency, now+1)
                else:
                    now = await connection.hget('short_block', center_frequency)
                    if now == None:
                        now = 0
                    await connection.hset('short_block', center_frequency, now+1)
                (blocks, loud) = ([], False)
                gc.collect()
    await sdr.stop()
    sdr.close()
    return None


def block_to_pulses(compressed_block: Info, smoother: FilterFunc = brickwall) -> np.ndarray:
    beep_samples = decompress(**compressed_block)
    shape = beep_samples.shape
    beep_absolute = np.empty(shape, dtype = 'float32')
    beep_absolute = np.abs(beep_samples)
    beep_smoothed = smoother(beep_absolute)
    threshold = 1.1*bn.nanmean(beep_smoothed)
    beep_binary = beep_smoothed > threshold
    pulses = rle(beep_binary)
    return np.array(pulses)

def pulses_to_deciles(pulses: np.ndarray) -> Deciles:
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

def pulses_to_breaks(pulses: np.ndarray, deciles: Deciles) -> typing.List[int]:
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
    deciles = pulses_to_deciles(pulses)
    if deciles == {}:
        return []
    breaks = pulses_to_breaks(pulses, deciles)
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
            rh = n[7]*16 + n[8]
            if n[0] == 5:
                return {'uid': uid, 'temperature': temp, 'humidity': rh, 'channel': channel}
    return {}


def pulses_to_packet(pulses: np.ndarray) -> typing.Dict:
    if len(pulses) > 10:
        for packet in demodulator(pulses):
            res = silver_sensor(packet)
            if 'uid' in res.keys():
                logging.debug(printer(packet.packet))
                res['uid'] = (res['channel']+1*1024)+res['uid']
                return res
    return {}

async def block_to_packet() -> typing.Awaitable[None]:
    connection = await init_redis()
    async for (timestamp, info) in pseudosub(connection, 'eof_timestamps'):
        if info is not None:
            for filter_func in filters:
                pulses = block_to_pulses(info, filter_func)
                decoded = pulses_to_packet(pulses)
                if decoded != {}:
                    break
        else:
            decoded = {}
        if decoded == {}:
            logging.debug('packetizer decoded %s %s' % (base64.b64encode(cbor.dumps(timestamp)).decode(), decoded))
            await connection.expire(timestamp, 3600)
            await connection.sadd('nontrivial_timestamps', [timestamp])
        else:
            logging.info('packetizer decoded %s %s' % (base64.b64encode(cbor.dumps(timestamp)).decode(), decoded))
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded['uid'], tsd.degc, float(decoded['temperature'])])
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded['uid'], tsd.rh, float(decoded['humidity'])])
            await connection.sadd('trivial_timestamps', [timestamp])
            await connection.expire(timestamp, 120)
    return None

def get_hardware_uid():
    import nacl.hash, nacl.encoding
    import glob
    import colourful_vegetables
    hashed = nacl.hash.sha512(open(glob.glob("/sys/block/mmcblk*/device/cid")[0], 'rb').read(), encoder=nacl.encoding.RawEncoder)
    colour = colourful_vegetables.colours[hashed[-1]>>4]
    veggy = colourful_vegetables.vegetables[hashed[-1]&0xF]
    human_name = "%s %s" % (colour, veggy)
    return human_name, hashed

def box_semisealed(to_box, public_key):
    import nacl
    session_key = nacl.public.PrivateKey.generate()
    session_box = nacl.public.Box(session_key, public_key)
    nonce = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
    boxed = session_box.encrypt(to_box, nonce)
    return session_key.public_key.encode()+boxed, session_box

def register_session_box(base_url = "http://localhost:8019"):
    import nacl.public
    import urllib.request
    relay_public_key = nacl.public.PublicKey(b'D\x8e\x9cT\x8b\xec\xb7\xf4\x17\xea\xa6\x8c\x11\xd3U\xb0\xbc\xe0\xb32\x15t\xbb\xe49^Y\xbf2\x8dUo')
    human_name, uid = get_hardware_uid()
    logging.info('human name: %s' % human_name)
    signed_message, session_box = box_semisealed(uid, relay_public_key)
    response = urllib.request.urlopen(base_url+'/register', data=signed_message)
    if response.getcode() != 200:
        raise Exception("sproutwave session key setup failed")
    return uid, session_box

async def packet_to_upstream(loop=None, host='data.sproutwave.com', udp_port=8019, https_port=8019, box=None):
    import socket
    import zlib
    if loop == None:
        loop = asyncio.get_event_loop()
    if box == None:
        uid, box = register_session_box(base_url = 'https://%s:%d' % (host, https_port))
    update_interval = 2
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    connection = await init_redis()
    samples = {}
    last_sent = time.time()
    async for (_, reading) in pseudosub(connection, 'upstream_channel'):
        if reading is not None:
            samples[reading[1]+4096*reading[2]] = reading
        if time.time() > (last_sent + update_interval):
            update_samples = [x for x in samples.values()]
            sock.sendto(box.encrypt(zlib.compress(cbor.dumps(update_samples))), (host, udp_port))
            last_sent = time.time()

async def packet_to_datastore() -> typing.Awaitable[None]:
    connection = await init_redis()
    datastore = tsd.TimeSeriesDatastore(db_name='sproutwave_v1.db')
    sys.stderr.write(datastore.db_name+'\n')
    async for (_, measurement) in pseudosub(connection, 'datastore_channel'):
        if measurement is not None:
            datastore.add_measurement(*measurement, raw=True)
    return None

def spawner(function_or_coroutine):
    logging.debug('spawning: ' + function_or_coroutine.__name__)
    def callable_wrapper(main):
        if asyncio.iscoroutinefunction(main):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(main())
        else:
            return main()
    process = multiprocessing.Process(target = callable_wrapper, args = (function_or_coroutine,))
    process.start()
    return process

async def band_monitor(connection = None):
    if connection == None:
        connection = await init_redis()
    while True:
        good_stats = await connection.hgetall_asdict('good_block')
        print("FREQ\t\tOK\tNO")
        for (freq,count) in sorted(good_stats.items()):
            failed = await connection.hget('short_block', freq)
            if failed == None:
                failed = 0
            print("%d\t%d\t%d" % (freq, count, failed))
        await asyncio.sleep(60)

async def watchdog(connection = None, threshold = 600, key = "sproutwave_ticks", send_pipe = None):
    if connection == None:
        connection = await init_redis()
    while True:
        now = time.time()
        ticks = await get_ticks(connection, key=key)
        for name, timestamp in ticks.items():
            logging.info("name: %s, now: %d, last_timestamp: %d" % (name, now, timestamp))
            if (now - timestamp) > 120 and name is not None:
                if send_pipe is not None:
                    send_pipe.send(name)
                return None
        await asyncio.sleep(60 - (time.time()-now))

def diag():
    print('pyvers: %s' % sys.hexversion)
    print('exec: %s' % sys.executable)
    print('pid: %d' % os.getpid())
    print('cwd: %s' % os.getcwd())
    print('prefix: %s' % sys.exec_prefix)
    print('sys.path: %s' % sys.path)

def start_redis_server():
    try:
        # check for running redis
        os.kill(int(open('/tmp/redis-server.pid', 'r').read()), 0)
    except:
        rconf = local_dir+'/resources/redis.conf'
        logging.info('launching redis with conf: %s' % rconf)
        redis = subprocess.Popen(['redis-server', rconf])

def main():
    diag()
    spawner(start_redis_server).join()
    funcs = (analog_to_block, block_to_packet, packet_to_datastore, packet_to_upstream, band_monitor)
    proc_mapping = {}
    while True:
        for func in funcs:
            name = func.__name__
            logging.info('(re)starting %s' % name)
            if name in proc_mapping.keys():
                proc_mapping[name].terminate()
            proc_mapping[name] = spawner(func)
        time.sleep(30)
        spawner(watchdog).join()

if __name__ == "__main__":
    main()
