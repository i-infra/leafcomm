import os
import sys
import time
import atexit
import typing
import pprint
import signal
import asyncio
import logging
import pathlib
import itertools
import encodings
import statistics
import subprocess
import multiprocessing

import cbor
import blosc
import numpy
import bottleneck
import ts_datastore as tsd

import handlebars


L, H, E = 0, 1, 2
data_dir = os.path.expanduser('~/.sproutwave/')

FilterFunc = typing.Callable[[numpy.ndarray], numpy.ndarray]
Info = typing.Dict
Deciles = typing.Dict[int, typing.Tuple[int, int]]
Packet = typing.NamedTuple('Packet', [('packet', list), ('errors', list), ('deciles', dict), ('raw', list)])


def complex_to_stereo(a): return numpy.dstack((a.real, a.imag))[0]


def stereo_to_complex(a): return a[0] + a[1] * 1j


def rerle(xs): return [(sum([i[0] for i in x[1]]), x[0]) for x in itertools.groupby(xs, lambda x: x[1])]


def rle(xs): return [(len(list(gp)), x) for x, gp in itertools.groupby(xs)]


def rld(xs): return itertools.chain.from_iterable((itertools.repeat(x, n) for n, x in xs))


def get_function_name(depth=0): return sys._getframe(depth + 1).f_code.co_name


def printer(xs): return ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])


def debinary(ba): return sum([x * 2 ** i for i, x in enumerate(reversed(ba))])


def brickwall(xs): return bottleneck.move_mean(xs, 32, 1)


local_dir = os.path.dirname(os.path.realpath(__file__))


try:
    import scipy
    from scipy.signal import butter, lfilter, freqz

    def butter_filter(data, cutoff, fs, btype='low', order=5):
        normalized_cutoff = cutoff / (0.5 * fs)
        if btype == 'bandpass':
            normalized_cutoff = [normalized_cutoff // 10, normalized_cutoff]
        b, a = butter(order, normalized_cutoff, btype=btype, analog=False)
        b = b.astype('float32')
        a = a.astype('float32')
        y = lfilter(b, a, data)
        return y

    def lowpass(xs): return butter_filter(xs, 2000.0, 256000.0, 'low', order=4)
    filters = [brickwall, lowpass]
except:
    filters = [brickwall]


def compress(in_): return (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__[
    'data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))


def decompress(size: int, dtype: str, data: bytes) -> numpy.ndarray:
    out = numpy.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__['data'][0])
    return out


def packed_bytes_to_iq(samples: bytes) -> numpy.ndarray:
    bytes_numpy = numpy.ctypeslib.as_array(samples)
    iq = bytes_numpy.astype(numpy.float32).view(numpy.complex64)
    iq /= 127.5
    iq -= 1 + 1j
    return iq


def init_redis(preface=''):
    return handlebars.init_redis(data_dir+preface+'sproutwave.sock')

async def tick(connection, function_name_depth=1, key='sproutwave_ticks'):
    name = get_function_name(function_name_depth)
    await connection.hset(key, name, time.time())


async def get_ticks(connection=None, key='sproutwave_ticks'):
    if connection == None:
        connection = await init_redis()
    resp = await connection.hgetall(key)
    return {x: float(y) for (x,y) in resp.items()}


async def pseudopub(connection, channels, timestamp = None, info = None):
    if isinstance(channels, str):
        channels = [channels]
    if not timestamp:
        timestamp = time.time()
    await connection.set(timestamp, cbor.dumps(info))
    for channel in channels:
        await connection.lpush(channel, timestamp)
    await connection.expireat(timestamp, int(timestamp + 600))


async def pseudosub(connection, channel, timeout=360):
    while True:
        yield await pseudosub1(connection, channel, timeout)


async def pseudosub1(connection, channel, timeout=360, depth=3, do_tick=True):
    channel, timestamp = await connection.brpop(channel, timeout)
    info = await connection.get(timestamp)
    if do_tick == True:
        await tick(connection, function_name_depth=depth)
    return (float(timestamp), cbor.loads(info))

def get_new_center():
    return int(433800000.0) + int(time.time() * 10) % 10 * 20000

async def analog_to_block() -> typing.Awaitable[None]:
    import gc
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()
    sdr.rs, sdr.fc, sdr.gain = 256000, 433900000.0, 9
    connection = await init_redis()
    block_size = 512 * 64
    samp_size = block_size // 2
    accumulating_blocks, blocks_accumulated, historical_power, sampled_background_block_count = False, [], 0, 1
    center_frequency = 433900000.0
    # primary sampling state machine
    # samples background amplitude, accumulate and transfer sample chunks significantly above ambient power
    async for byte_samples in sdr.stream(block_size, format='bytes'):
        await tick(connection)
        complex_samples = packed_bytes_to_iq(byte_samples)
        # calculate cum and avg power
        block_power = numpy.sum(numpy.abs(complex_samples))
        historical_background_power = historical_power / sampled_background_block_count
        # print center frequency and reset accumulators every thousand blocks (if not accumulating_blocks)
        if sampled_background_block_count % 1000 == 1 and accumulating_blocks is False:
            # reset threshold every 10k blocks
            if sampled_background_block_count > 10000:
                historical_power, sampled_background_block_count = historical_background_power, 1
            center_frequency = get_new_center()
            sdr.set_center_freq(center_frequency)
            logging.info('center frequency: %d' % sdr.get_center_freq())
        # init accumulator safely
        if historical_power == 0:
            historical_power = block_power
            sampled_background_block_count += 1
        if block_power > historical_background_power:
            if accumulating_blocks is False:
                timestamp = time.time()
            historical_power += (block_power - historical_background_power) / 100.0
            accumulating_blocks = True
            blocks_accumulated.append(complex_samples)
        else:
            historical_power += block_power
            sampled_background_block_count += 1
            if accumulating_blocks is True:
                if len(blocks_accumulated) > 9:
                    blocks_accumulated.append(complex_samples)
                    sampled_message = numpy.concatenate(blocks_accumulated)
                    size, dtype, compressed = compress(sampled_message)
                    info = {'size': size, 'dtype': dtype.name, 'data': compressed}
                    await pseudopub(connection, 'eof_timestamps', timestamp, info)
                    logging.debug('flushing: %s' % {'duration': time.time() - timestamp, 'center_freq': center_frequency, 'block_count': len(blocks_accumulated), 'message power': numpy.sum(numpy.abs(sampled_message)) / len(blocks_accumulated), 'avg': historical_background_power, 'lifetime_blocks': sampled_background_block_count})
                    await connection.hincrby('good_block', center_frequency, 1)
                else:
                    await connection.hincrby('short_block', center_frequency, 1)
                blocks_accumulated, accumulating_blocks = [], False
                gc.collect()
    await sdr.stop()
    sdr.close()
    return None


def block_to_pulses(compressed_block: Info, smoother: FilterFunc=brickwall) -> numpy.ndarray:
    beep_samples = decompress(**compressed_block)
    shape = beep_samples.shape
    beep_absolute = numpy.empty(shape, dtype='float32')
    beep_absolute = numpy.abs(beep_samples)
    beep_smoothed = smoother(beep_absolute)
    threshold = 1.1 * bottleneck.nanmean(beep_smoothed)
    beep_binary = beep_smoothed > threshold
    pulses = rle(beep_binary)
    return numpy.array(pulses)


def pulses_to_deciles(pulses: numpy.ndarray) -> Deciles:
    values = numpy.unique(pulses.T[1])
    deciles = {}
    if len(pulses) < 10:
        return {}
    for value in values:
        counts = numpy.sort(pulses[numpy.where(pulses.T[1] == value)].T[0])
        tenth = len(counts) // 10
        if not tenth:
            return {}
        short_decile = int(numpy.mean(counts[1 * tenth:2 * tenth]))
        long_decile = int(numpy.mean(counts[8 * tenth:9 * tenth]))
        deciles[value] = short_decile, long_decile
    return deciles


def pulses_to_breaks(pulses: numpy.ndarray, deciles: Deciles) -> typing.List[int]:
    cutoff = min(deciles[0][0], deciles[1][0]) * 9
    breaks = numpy.logical_and(pulses.T[0] > cutoff, pulses.T[1] == L).nonzero()[0]
    break_deltas = numpy.diff(breaks)
    if len(breaks) > 50 or len(breaks) < 5:
        return []
    elif len(numpy.unique(break_deltas[numpy.where(break_deltas > 3)])) > 3:
        try:
            d_mode = statistics.mode([bd for bd in break_deltas if bd > 3])
        except statistics.StatisticsError:
            d_mode = round(statistics.mean(break_deltas))
        expected_breaks = [x * d_mode for x in range(round(max(breaks) // d_mode))]
        if len(expected_breaks) < 2:
            return []
        tolerance = d_mode // 10
        breaks = [x for x, bd in zip(breaks, break_deltas) if True in [abs(x - y) < tolerance for y in expected_breaks] or bd == d_mode or bd < 5]
    return breaks


def demodulator(pulses: numpy.ndarray) -> typing.Iterable[Packet]:
    deciles = pulses_to_deciles(pulses)
    if deciles == {}:
        return []
    breaks = pulses_to_breaks(pulses, deciles)
    if len(breaks) == 0:
        return []
    for x, y in zip(breaks, breaks[1:]):
        pulse_group = pulses[x + 1:y]
        bits = []
        errors = []
        for chip in pulse_group:
            valid = False
            for v in deciles.keys():
                for i, width in enumerate(deciles[v]):
                    if not valid and chip[1] == v and abs(chip[0] - width) < width // 2:
                        bits += [v] * (i + 1)
                        valid = True
            if not valid:
                errors += [chip]
                bits.append(E)
        if len(bits) > 4:
            result = Packet(bits, errors, deciles, pulses[x:y])
            yield result


def silver_sensor(packet: Packet) -> typing.Dict:
    if packet.errors == []:
        bits = [x[0] == 2 for x in rle(packet.packet) if x[1] == 0]
        if len(bits) == 42:
            fields = [0, 2, 8, 2, 2, 4, 4, 4, 4, 4, 8]
            fields = [x for x in itertools.accumulate(fields)]
            results = [debinary(bits[x:y]) for x, y in zip(fields, fields[1:])]
            if results[1] == 255:
                return {}
            temp = 16 ** 2 * results[6] + 16 * results[5] + results[4]
            humidity = 16 * results[8] + results[7]
            if temp > 1000:
                temp %= 1000
                temp += 100
            temp /= 10
            temp -= 32
            temp *= 5 / 9
            return {'uid': results[1], 'temperature': temp, 'humidity': humidity, 'channel': results[3]}
        elif len(bits) == 36:
            fields = [0] + [4] * 9
            fields = [x for x in itertools.accumulate(fields)]
            n = [debinary(bits[x:y]) for x, y in zip(fields, fields[1:])]
            model = n[0]
            uid = n[1] << 4 | n[2]
            temp = n[4] << 8 | n[5] << 4 | n[6]
            if temp >= 3840:
                temp -= 4096
            channel = n[3] & 3
            battery_ok = 8 & n[2] == 0
            temp /= 10
            rh = n[7] * 16 + n[8]
            if n[0] == 5:
                return {'uid': uid, 'temperature': temp, 'humidity': rh, 'channel': channel}
    return {}


def pulses_to_packet(pulses: numpy.ndarray) -> typing.Dict:
    if len(pulses) > 10:
        for packet in demodulator(pulses):
            res = silver_sensor(packet)
            if 'uid' in res.keys():
                logging.debug(printer(packet.packet))
                res['uid'] = res['channel'] + 1 * 1024 + res['uid']
                return res
    return {}


async def block_to_packet() -> typing.Awaitable[None]:
    connection = await init_redis()
    async for timestamp, info in pseudosub(connection, 'eof_timestamps'):
        if info is not None:
            for filter_func in filters:
                pulses = block_to_pulses(info, filter_func)
                decoded = pulses_to_packet(pulses)
                if decoded != {}:
                    break
        else:
            decoded = {}
        if decoded == {}:
            logging.debug('packetizer decoded %s %s' % (timestamp, decoded))
            await connection.expire(timestamp, 3600)
            await connection.sadd('nontrivial_timestamps', timestamp)
        else:
            logging.info('packetizer decoded %s %s' % (timestamp, decoded))
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded['uid'], tsd.degc, float(decoded['temperature'])])
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded['uid'], tsd.rh, float(decoded['humidity'])])
            await connection.sadd('trivial_timestamps', timestamp)
            await connection.hincrby('sensor_uuid_counts', decoded['uid'], 1)
            await connection.hset('sensor_uuid_timestamps', decoded['uid'], timestamp)
            await connection.expire(timestamp, 120)
    return None


def get_hardware_uid() -> (str, bytes):
    import nacl.hash
    import nacl.encoding
    import glob
    import colourful_vegetables
    hashed = nacl.hash.sha512(open(glob.glob('/sys/block/mmcblk*/device/cid')[0], 'rb').read(), encoder=nacl.encoding.RawEncoder)
    colour = colourful_vegetables.colours[hashed[-1] >> 4]
    veggy = colourful_vegetables.vegetables[hashed[-1] & 15]
    human_name = '%s %s' % (colour, veggy)
    return (human_name, hashed)


def create_box_semisealed(to_box, public_key):
    import nacl
    session_key = nacl.public.PrivateKey.generate()
    session_box = nacl.public.Box(session_key, public_key)
    session_id = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
    nonce = nacl.utils.random(nacl.public.Box.NONCE_SIZE)
    boxed = session_box.encrypt(session_id+to_box, nonce)
    return (session_key.public_key.encode() + boxed, session_id, session_box)


def register_session_box(base_url='http://localhost:8019'):
    import nacl.public
    import urllib.request
    relay_public_key = nacl.public.PublicKey(b'D\x8e\x9cT\x8b\xec\xb7\xf4\x17\xea\xa6\x8c\x11\xd3U\xb0\xbc\xe0\xb32\x15t\xbb\xe49^Y\xbf2\x8dUo')
    human_name, uid = get_hardware_uid()
    logging.info('human name: %s' % human_name)
    signed_message, session_id, session_box = create_box_semisealed(uid, relay_public_key)
    while True:
        try:
            response = urllib.request.urlopen(base_url + '/register', data=signed_message)
            if response.getcode() != 200:
                raise Exception('sproutwave session key setup failed')
            else:
                return (uid, session_id, session_box)
        except:
            time.sleep(60)


async def packet_to_upstream(loop=None, host='data.sproutwave.com', udp_port=8019, https_port=8019, box=None):
    import socket
    import zlib
    if loop == None:
        loop = asyncio.get_event_loop()
    if box == None:
        uid, session_id, box = register_session_box(base_url='https://%s:%d' % (host, https_port))
    max_update_interval = 1
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    connection = await init_redis()
    samples = {}
    last_sent = time.time()
    async for _, reading in pseudosub(connection, 'upstream_channel'):
        seen_multiple_times_recently = False
        if reading:
            seen_count = int(await connection.hget('sensor_uuid_counts', reading[1]) or '0')
            last_seen = float(await connection.hget('sensor_uuid_timestamps', reading[1]) or '0')
            if last_seen and seen_count:
                seen_multiple_times_recently = (seen_count > 1) and ((float(reading[0]) - last_seen) < 600)
        if seen_multiple_times_recently:
            if reading:
                samples[reading[1] + 4096 * reading[2]] = reading
            if (time.time() > last_sent + max_update_interval):
                update_samples = [x for x in samples.values()]
                logging.info(pprint.pformat(update_samples))
                sock.sendto(session_id+box.encrypt(zlib.compress(cbor.dumps(update_samples))), (host, udp_port))
                last_sent = time.time()


async def packet_to_datastore() -> typing.Awaitable[None]:
    connection = await init_redis()
    datastore = tsd.TimeSeriesDatastore(db_name=data_dir + 'sproutwave_v1.db')
    sys.stderr.write(datastore.db_name + '\n')
    async for _, measurement in pseudosub(connection, 'datastore_channel'):
        if measurement is not None:
            datastore.add_measurement(*measurement, raw=True)
    return None

async def band_monitor(connection=None):
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        good_stats = await connection.hgetall('good_block')
        print('FREQ\t\tOK\tNO')
        for freq, count in sorted(good_stats.items()):
            failed = await connection.hget('short_block', freq)
            if failed == None:
                failed = b'0'
            print(f'{freq.decode()}\t{count.decode()}\t{failed.decode()}')
        await asyncio.sleep(60)

async def sensor_monitor(connection=None):
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        sensor_uuid_counts = await connection.hgetall('sensor_uuid_counts')
        sensor_last_seen = await connection.hgetall('sensor_uuid_timestamps')
        now = time.time()
        print('UUID\tCOUNT\tLAST SEEN')
        for uid, count in sorted(sensor_uuid_counts.items()):
            print('%s\t%s\t%d' % (uid.decode(), count.decode(), now-float(sensor_last_seen[uid])))
        await asyncio.sleep(60)

async def watchdog(connection=None, threshold=600, key='sproutwave_ticks', send_pipe=None):
    timeout = 600
    tick_cycle = 120
    if connection == None:
        connection = await init_redis()
    while True:
        now = time.time()
        ticks = await get_ticks(connection, key=key)
        for name, timestamp in ticks.items():
            logging.info('name: %s, now: %d, last_timestamp: %d' % (name.decode(), now, timestamp))
            if (now - timestamp) > timeout and name:
                if send_pipe:
                    send_pipe.send(name)
                return
        await asyncio.sleep(tick_cycle - (time.time() - now))


def diag():
    _diag = {'pyvers': sys.hexversion, 'exec': sys.executable, 'pid': os.getpid(), 'cwd': os.getcwd(), 'prefix': sys.exec_prefix, 'sys.path': sys.path}
    logging.info(str(_diag))
    return _diag

def main():
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    debug = '--debug' in sys.argv
    if debug:
        diag()
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.getLogger().setLevel(log_level)
    redis_server_process = handlebars.start_redis_server(redis_socket_path=data_dir+'sproutwave.sock')
    funcs = analog_to_block, block_to_packet, packet_to_datastore, packet_to_upstream, band_monitor, sensor_monitor
    proc_mapping = {}
    while True:
        for func in funcs:
            name = func.__name__
            logging.info('(re)starting %s' % name)
            if name in proc_mapping.keys():
                proc_mapping[name].terminate()
            proc_mapping[name] = handlebars.multi_spawner(func)
        time.sleep(30)
        handlebars.multi_spawner(watchdog).join()

if __name__ == "__main__":
    main()
