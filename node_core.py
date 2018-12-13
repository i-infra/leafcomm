import os
import sys
import time
import typing
import pprint
import asyncio
import pathlib
import itertools
import statistics
import dataclasses

import blosc
import numpy
import bottleneck
import ts_datastore as tsd

from node_comms import *

import handlebars
import _constants

L, H, E = 0, 1, 2
data_dir = os.path.expanduser('~/.sproutwave/')

FilterFunc = typing.Callable[[numpy.ndarray], numpy.ndarray]
Deciles = typing.Dict[int, typing.Tuple[int, int]]

@dataclasses.dataclass
class Packet:
    bits: list
    errors: list
    deciles: dict
    raw: list
    timestamp: float

@dataclasses.dataclass
class Pulses:
    sequence: list
    timestamp: float

@dataclasses.dataclass
class CompressedAnalog:
    size: int
    dtype: str
    data: bytes
    timestamp: float

logger = handlebars.get_logger(__name__, debug='--debug' in sys.argv)


def complex_to_stereo(a):
    return numpy.dstack((a.real, a.imag))[0]


def stereo_to_complex(a):
    return a[0] + a[1] * 1j


def printer(xs):
    return ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])


def brickwall(xs):
    return bottleneck.move_mean(xs, 32, 1)


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

    def lowpass(xs):
        return butter_filter(xs, 2000.0, 256000.0, 'low', order=4)

    filters = [brickwall, lowpass]
except:
    filters = [brickwall]


def compress(in_):
    return (in_.size, in_.dtype.str,
            blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))


def decompress(size: int, dtype: str, data: bytes, **kwargs) -> numpy.ndarray:
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
    return handlebars.init_redis(data_dir + preface + 'sproutwave.sock')


def get_new_center():
    return 433_800_000 + handlebars.r1dx(10) * 20_000


async def analog_to_block() -> typing.Awaitable[None]:
    import gc
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()
    sdr.rs, sdr.fc, sdr.gain = 256000, 433900000.0, 15
    connection = await init_redis()
    block_size = 512 * 64
    samp_size = block_size // 2
    accumulating_blocks, blocks_accumulated, historical_power, sampled_background_block_count = False, [], 0, 1
    center_frequency = 433900000.0
    # primary sampling state machine
    # samples background amplitude, accumulate and transfer sample chunks significantly above ambient power
    async for byte_samples in sdr.stream(
        block_size, format='bytes'):
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
            logger.info('center frequency: %d' % sdr.get_center_freq())
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
                    info = CompressedAnalog(*compress(sampled_message), timestamp)
                    await pseudopub(connection, 'eof_timestamps', timestamp, dataclasses.asdict(info))
                    logger.debug(
                        'flushing: %s' % {
                            'timestamp': timestamp,
                            'duration_ms': int((time.time() - timestamp)*1000),
                            'center_freq': center_frequency,
                            'block_count': len(blocks_accumulated),
                            'message power': int(numpy.sum(numpy.abs(sampled_message)) / len(blocks_accumulated)),
                            'avg': int(historical_background_power),
                            'lifetime_blocks': sampled_background_block_count
                        })
                    await connection.hincrby('success_block', center_frequency, 1)
                else:
                    await connection.hincrby('fail_block', center_frequency, 1)
                blocks_accumulated, accumulating_blocks = [], False
                gc.collect()
    await sdr.stop()
    sdr.close()
    return None


def block_to_pulses(compressed_block: CompressedAnalog, smoother: FilterFunc = brickwall) -> Pulses:
    """ takes a compressed block of analog samples, takes magnitude, amplitude-thresholds and returns pulses """
    beep_samples = decompress(**compressed_block)
    beep_absolute = numpy.abs(beep_samples)
    beep_smoothed = smoother(beep_absolute)
    threshold = 1.1 * bottleneck.nanmean(beep_smoothed)
    beep_binary = beep_smoothed > threshold
    return Pulses(numpy.array(handlebars.rle(beep_binary)), compressed_block.get('timestamp'))


def pulses_to_deciles(pulses: Pulses) -> Deciles:
    """ takes an array of run-length encoded pulses, finds centroids for 'long' and 'short' """
    values = numpy.unique(pulses.sequence.T[1])
    deciles = {}
    if len(pulses.sequence) < 10:
        return {}
    for value in values:
        counts = numpy.sort(pulses.sequence[numpy.where(pulses.sequence.T[1] == value)].T[0])
        tenth = len(counts) // 10
        if not tenth:
            return {}
        short_decile = int(numpy.mean(counts[1 * tenth:2 * tenth]))
        long_decile = int(numpy.mean(counts[8 * tenth:9 * tenth]))
        deciles[value] = short_decile, long_decile
    logger.debug(f'using deciles {deciles}')
    return deciles


def pulses_to_breaks(pulses: Pulses, deciles: Deciles) -> typing.List[int]:
    """ with pulse array and definition of 'long' and 'short' pulses, find position of repeated packets """
    # heuristic - cutoff duration for a break between subsequent packets is at least 9x
    #  the smallest of the 'long' and 'short' pulses
    cutoff = min(deciles[0][0], deciles[1][0]) * 9
    breaks = numpy.logical_and(pulses.sequence.T[0] > cutoff, pulses.sequence.T[1] == L).nonzero()[0]
    break_deltas = numpy.diff(breaks)
    # heuristic - "packet" "repeated" with fewer than 5 separations or more than 50 breaks is no good
    if len(breaks) > 50 or len(breaks) < 5:
        breaks = []
    # heuristic - if more than 3 different spacings between detected repeated packets, tighten up purposed breaks
    elif len(numpy.unique(break_deltas[numpy.where(break_deltas > 3)])) > 3:
        try:
            d_mode = statistics.mode([bd for bd in break_deltas if bd > 3])
        except statistics.StatisticsError:
            d_mode = round(statistics.mean(break_deltas))
        expected_breaks = [x * d_mode for x in range(round(max(breaks) // d_mode))]
        if len(expected_breaks) < 2:
            return []
        tolerance = d_mode // 10
        breaks = [x for x, bd in zip(breaks, break_deltas) if any([abs(x - y) < tolerance for y in expected_breaks]) or bd == d_mode or bd < 5]
    logger.debug(f'using breaks {breaks}')
    return breaks


def demodulator(pulses: Pulses) -> typing.Iterable[Packet]:
    """ high level demodulator function accepting a pulse array and yielding 0 or more Packets """
    deciles = pulses_to_deciles(pulses)
    if deciles == {}:
        return []
    breaks = pulses_to_breaks(pulses, deciles)
    if len(breaks) == 0:
        return []
    for x, y in zip(breaks, breaks[1:]):
        pulse_group = pulses.sequence[x + 1:y]
        symbols = []
        errors = []
        for chip in pulse_group:
            valid = False
            for v in deciles.keys():
                for i, width in enumerate(deciles[v]):
                    if not valid and chip[1] == v and abs(chip[0] - width) < width // 2:
                        symbols += [v] * (i + 1)
                        valid = True
            if not valid:
                errors += [chip]
                symbols.append(E)
        # heuristic - don't have packets with fewer than 10 symbols
        if E not in symbols and len(symbols) > 10:
            # NB. this is somewhat unique to the silver sensors in question
            # duration of low pulses determines bit value 100 -> 1; 10 -> 0
            bits = [x[0] == 2 for x in handlebars.rle(symbols) if x[1] == 0]
            yield Packet(bits, errors, deciles, pulses.sequence[x:y], pulses.timestamp)


@dataclasses.dataclass
class SilverSensor_40b:
    # s3318p
    framing: int
    uid: int
    unk: int
    channel: int
    temp0: int
    temp1: int
    temp2: int
    rh0: int
    rh1: int
    button_pushed: bool
    low_battery: bool
    checksum: int


@dataclasses.dataclass
class SilverSensor_36b:
    # prologue
    model: int
    uid: int
    channel: int
    low_battery: bool
    button_pushed: bool
    temp: int
    rh: int


@dataclasses.dataclass
class Sample:
    uid: int
    temperature: float
    humidity: float
    channel: int
    low_battery: bool
    button_pushed: bool


def silver_sensor(packet: Packet) -> typing.Dict:
    """ hardware specific demodulation function for the two types of silver sensors """
    success = False
    if packet.errors == []:
        if len(packet.bits) == 42:
            field_bitwidths = [0, 2, 8, 2, 2, 4, 4, 4, 4, 4, 1, 1, 6]
            field_positions = [x for x in itertools.accumulate(field_bitwidths)]
            results = [handlebars.debinary(packet.bits[x:y]) for x, y in zip(field_positions, field_positions[1:])]
            if results[1] == 255:
                return {}
            n = SilverSensor_40b(*results)
            temp = 16**2 * n.temp2 + 16 * n.temp1 + n.temp0
            humidity = 16 * n.rh1 + n.rh0
            if temp > 1000:
                temp %= 1000
                temp += 100
            temp /= 10
            temp -= 32
            temp *= 5 / 9
            success = True
        elif len(packet.bits) == 36:
            field_bitwidths = [0, 4, 8, 2, 1, 1, 12, 8]
            field_positions = [x for x in itertools.accumulate(field_bitwidths)]
            results = [handlebars.debinary(packet.bits[x:y]) for x, y in zip(field_positions, field_positions[1:])]
            n = SilverSensor_36b(*results)
            temp = n.temp
            if temp >= 3840:
                temp -= 4096
            temp /= 10
            humidity = n.rh
            if n.model == 5:
                success = True
    if success:
        if len(packet.bits) == 42:
            open('42_bits', 'a').write(''.join([{True: '1', False: '0'}[bit] for bit in packet.bits]) + '\n')
        return Sample(n.uid+1024+n.channel, temp, humidity, n.channel, n.low_battery, n.button_pushed)


def pulses_to_sample(pulses: Pulses) -> Sample:
    """ high-level function accepting RLE'd pulses and returning a sample from a silver sensor if present """
    # heuristic - don't process short collections of pulses
    if len(pulses.sequence) < 10:
        return
    packet_possibilities = []
    for packet in demodulator(pulses):
        logger.debug(f'got: {len(packet.bits)} {printer(packet.bits)}')
        res = silver_sensor(packet)
        packet_possibilities += [res]
    for possibility in packet_possibilities:
        if packet_possibilities.count(possibility) > 1:
            return possibility


async def block_to_sample() -> typing.Awaitable[None]:
    """ worker function which waits for blocks of samples in redis queue, attempts to demodulate / decode to T&H.
    does housekeeping"""
    connection = await init_redis()
    async for timestamp, compressed_raw_samples in pseudosub(connection, 'eof_timestamps'):
        decoded = None
        if compressed_raw_samples:
            for filter_func in filters:
                pulses = block_to_pulses(compressed_raw_samples, filter_func)
                decoded = pulses_to_sample(pulses)
                if decoded:
                    break
        logger.info(f'decoded {timestamp} {decoded} with {filter_func}')
        if decoded:
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded.uid, tsd.degc, float(decoded.temperature)])
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded.uid, tsd.rh, float(decoded.humidity)])
            await connection.sadd('success_timestamps', timestamp)
            await connection.hincrby('sensor_uuid_counts', decoded.uid, 1)
            await connection.hset('sensor_uuid_timestamps', decoded.uid, timestamp)
            await connection.expire(timestamp, 120)
        else:
            await connection.expire(timestamp, 3600)
            await connection.sadd('fail_timestamps', timestamp)
    return None


def get_hardware_uid() -> (str, bytes):
    """ get human name and hash of SD card CID """
    import nacl.hash
    import nacl.encoding
    import glob
    import colourful_vegetables
    hashed = nacl.hash.sha256(open(glob.glob('/sys/block/mmcblk*/device/cid')[0], 'rb').read(), encoder=nacl.encoding.RawEncoder)[0:8]
    colour = colourful_vegetables.colours[hashed[-1] >> 4]
    veggy = colourful_vegetables.vegetables[hashed[-1] & 15]
    human_name = '%s %s' % (colour, veggy)
    return (human_name, hashed)


async def register_session_box():
    """ register node with backend. """
    import aiohttp
    human_name, uid = get_hardware_uid()
    logger.info('human name: %s' % human_name)
    packer, unpacker = get_packer_unpacker(await init_redis(), _constants.upstream_pubkey_bytes)
    encrypted_msg = await packer(uid)
    url = f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/register'
    async with aiohttp.ClientSession() as session:
        async with session.post(url=url, data=encrypted_msg) as resp:
            if resp.status == 200:
                encrypted_status = await resp.read()
                status = await unpacker(encrypted_status)
                return (uid, packer)
            else:
                raise Exception('sproutwave session key setup failed')


async def sample_to_upstream(loop=None):
    """ accumulates frames of latest values and periodically encrypts and sends to upstream via udp """
    connection = await init_redis()
    await tick(connection)
    import socket
    if loop == None:
        loop = asyncio.get_event_loop()
    uid, packer = await register_session_box()
    max_update_interval = 1
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    samples = {}
    last_sent = time.time()
    async for timestamp, reading in pseudosub(connection, 'upstream_channel'):
        seen_multiple_times_recently = False
        if reading:
            seen_count = int(await connection.hget('sensor_uuid_counts', reading[1]) or '0')
            last_seen = float(await connection.hget('sensor_uuid_timestamps', reading[1]) or '0')
            if last_seen and seen_count:
                seen_multiple_times_recently = (seen_count > 2) and ((float(reading[0]) - last_seen) < 600)
        if seen_multiple_times_recently:
            if reading:
                samples[reading[1] + 4096 * reading[2]] = reading
            if (time.time() > last_sent + max_update_interval):
                update_samples = [x for x in samples.values()]
                logger.info(f'submitting {pprint.pformat(update_samples)}')
                update_message = await packer(update_samples)
                sock.sendto(update_message, (_constants.upstream_host, _constants.upstream_port))
                last_sent = time.time()


async def sample_to_datastore() -> typing.Awaitable[None]:
    """ logs samples in the local sqlite database """
    connection = await init_redis()
    datastore = tsd.TimeSeriesDatastore(db_name=data_dir + 'sproutwave_v1.db')
    sys.stderr.write(datastore.db_name + '\n')
    async for _, reading in pseudosub(connection, 'datastore_channel'):
        if reading is not None:
            datastore.add_measurement(*reading, raw=True)
    return None


async def band_monitor(connection=None):
    """ periodically log a table of how many blocks we've successfully or unsuccessfully decoded for all frequencies """
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        good_stats = await connection.hgetall('success_block')
        msg = ['FREQ\t\tOK\tNO']
        for freq, count in sorted(good_stats.items()):
            failed = await connection.hget('fail_block', freq)
            if failed == None:
                failed = b'0'
            msg += [f'{freq.decode()}\t{count.decode()}\t{failed.decode()}']
        logger.info('\n'.join(msg))
        await asyncio.sleep(60)


async def sensor_monitor(connection=None):
    """ periodically log a table of last seen and number times seen for all sensors """
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        sensor_uuid_counts = await connection.hgetall('sensor_uuid_counts')
        sensor_last_seen = await connection.hgetall('sensor_uuid_timestamps')
        now = time.time()
        msg = ['UUID\tCOUNT\tLAST SEEN']
        for uid, count in sorted(sensor_uuid_counts.items()):
            msg += ['%s\t%s\t%d' % (uid.decode(), count.decode(), now - float(sensor_last_seen[uid]))]
        logger.info('\n'.join(msg))
        await asyncio.sleep(60)


async def watchdog(connection=None, threshold=600, key='sproutwave_ticks', send_pipe=None):
    """ run until it's been more than 600 seconds since all flowgraphs ticked, then exit """
    timeout = 600
    tick_cycle = 120
    if connection == None:
        connection = await init_redis()
    while True:
        now = time.time()
        ticks = await get_ticks(connection, key=key)
        for name, timestamp in ticks.items():
            logger.info('name: %s, now: %d, last_timestamp: %d' % (name.decode(), now, timestamp))
            if (now - timestamp) > timeout and name:
                if send_pipe:
                    send_pipe.send(name)
                return
        await asyncio.sleep(tick_cycle - (time.time() - now))


def diag():
    _diag = {'pyvers': sys.hexversion, 'exec': sys.executable, 'pid': os.getpid(), 'cwd': os.getcwd(), 'prefix': sys.exec_prefix, 'sys.path': sys.path}
    logger.info(str(_diag))
    return _diag


def main():
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    redis_server_process = handlebars.start_redis_server(redis_socket_path=data_dir + 'sproutwave.sock')
    funcs = analog_to_block, block_to_sample, sample_to_datastore, sample_to_upstream, band_monitor, sensor_monitor
    proc_mapping = {}
    while True:
        for func in funcs:
            name = func.__name__
            logger.info('(re)starting %s' % name)
            if name in proc_mapping.keys():
                proc_mapping[name].terminate()
            proc_mapping[name] = handlebars.multi_spawner(func)
        time.sleep(30)
        handlebars.multi_spawner(watchdog).join()


if __name__ == "__main__":
    main()
