import os
import sys
import time
import json
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


sampling_rate = 256_000
L, H, E = 0, 1, 2
logger = handlebars.get_logger(__name__, debug='--debug' in sys.argv)

FilterFunc = typing.Callable[[numpy.ndarray], numpy.ndarray]
Deciles = typing.Dict[int, typing.Tuple[int, int]]

@dataclasses.dataclass
class Packet:
    bits: list
    errors: list
    deciles: dict
    raw: list
    ulid: str

@dataclasses.dataclass
class Pulses:
    sequence: list
    ulid: str


def printer(xs):
    return ''.join([{L: '░', H: '█', E: '╳'}[x] for x in xs])


def brickwall(xs):
    return bottleneck.move_mean(xs, 32, 1)


try:
    import scipy
    from scipy.signal import butter, lfilter, freqz

    def build_butter_filter(cutoff, fs, btype='low', order=5):
        normalized_cutoff = cutoff / (0.5 * fs)
        if btype == 'bandpass':
            normalized_cutoff = [normalized_cutoff // 10, normalized_cutoff]
        b, a = butter(order, normalized_cutoff, btype=btype, analog=False)
        b = b.astype('float32')
        a = a.astype('float32')
        return b, a
    signal_filter_coefficients = dict(zip(('b', 'a'), build_butter_filter(2000, sampling_rate, order=4)))
    def lowpass(xs):
        return lfilter(signal_filter_coefficients['b'], signal_filter_coefficients['a'], xs)
    filters = [brickwall, lowpass]
except:
    filters = [brickwall]


def compress(in_):
    return dict(size=in_.size, dtype=in_.dtype.str,
            data=blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))


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


def get_new_center():
    return 433_800_000 + handlebars.r1dx(10) * 20_000


async def analog_to_block() -> typing.Awaitable[None]:
    # this implements the meat of the radio receiving and monitoring
    # * streams data from the RTL-SDR hardware
    # * models background noise
    # * accumulates and enqueues compressed recordings of transmissions for future processing
    # uses rtlsdraio, node_comms, and numpy
    # lowpass filter tracking amplitude
    import gc
    from rtlsdr import rtlsdraio
    sdr = rtlsdraio.RtlSdrAio()
    sdr.rs, sdr.fc, sdr.gain = sampling_rate, 433900000.0, 9
    connection = await init_redis()
    # number of bytes
    block_size = 512 * 64
    samp_size = block_size // 2
    # initial state
    accumulating_blocks, blocks_accumulated, historical_power, sampled_background_block_count = False, [], 0, 0
    center_frequency = 433900000.0
    # primary sampling state machine
    # samples background amplitude, accumulate and transfer sample chunks significantly above ambient power
    async for byte_samples in sdr.stream(block_size, format='bytes'):
        last_tick = float(await connection.hget(f'{redis_prefix}_function_call_ticks', handlebars.get_function_name()) or b'0')
        await tick(connection)
        now_tick = float(await connection.hget(f'{redis_prefix}_function_call_ticks', handlebars.get_function_name()) or b'0')
        complex_samples = packed_bytes_to_iq(byte_samples)
        # calculate cum and avg power
        block_power = numpy.sum(numpy.abs(complex_samples))
        # init accumulator safely
        if historical_power == 0:
            historical_power = block_power
            logger.info(f"INIT WITH HISTORICAL POWER {historical_power}")
            sampled_background_block_count += 1
        historical_background_power = historical_power / sampled_background_block_count
        #  do background ops if not accumulating data
        if sampled_background_block_count % 1000 == 1 and accumulating_blocks is False:
            # reset threshold every 10k blocks
            if sampled_background_block_count > 10000:
                logger.info(f'resetting accumulator to {int(historical_background_power)}')
                historical_power, sampled_background_block_count = historical_background_power, 1
            center_frequency = get_new_center()
            sdr.set_center_freq(center_frequency)
            center_frequency = sdr.get_center_freq()
            logger.info('center frequency: %d' % center_frequency)
        if block_power > historical_background_power * 1.02:
            if accumulating_blocks is False:
                logger.info(f"STARTING ACCUMULATION with {block_power} compared to {historical_background_power}")
                start_of_transmission_timestamp = time.time()
            historical_power += (block_power - historical_background_power) / 100.0
            accumulating_blocks = True
            blocks_accumulated.append(complex_samples)
        else:
            historical_power += block_power
            sampled_background_block_count += 1
            if accumulating_blocks is True:
                len_accumulated = len(blocks_accumulated)
                if len_accumulated > 9 and len_accumulated < 40:
                    end_of_transmission_timestamp = time.time()
                    blocks_accumulated.append(complex_samples)
                    sampled_message = numpy.concatenate(blocks_accumulated)
                    info = compress(sampled_message)
                    info['center_frequency'] = center_frequency
                    ulid = await pseudopub(connection, 'transmissions', timestamp=start_of_transmission_timestamp, reading=info)
                    logger.debug(
                        'flushing: %s' % {
                            'start_timestamp': start_of_transmission_timestamp,
                            'duration_ms': int((end_of_transmission_timestamp - start_of_transmission_timestamp)*1000),
                            'center_freq': center_frequency,
                            'block_count': len(blocks_accumulated),
                            'message power': int(numpy.sum(numpy.abs(sampled_message)) / len(blocks_accumulated)),
                            'avg': int(historical_background_power),
                            'lifetime_blocks': sampled_background_block_count,
                            'iteration_duration': time.time()-now_tick,
                            'ulid': ulid
                        })
                    await connection.hincrby(f'{redis_prefix}_found_maybe_good_transmission_at_frequency', center_frequency, 1)
                else:
                    await connection.hincrby(f'{redis_prefix}_found_bad_transmission_at_frequency', center_frequency, 1)
                blocks_accumulated, accumulating_blocks = [], False
                gc.collect()
    await sdr.stop()
    sdr.close()
    return None


def block_to_pulses(compressed_iq_reading: SerializedReading, smoother: FilterFunc = brickwall) -> Pulses:
    """ takes a compressed block of analog samples, takes magnitude, amplitude-thresholds and returns pulses """
    beep_samples = decompress(**compressed_iq_reading.value)
    beep_absolute = numpy.abs(beep_samples)
    beep_smoothed = smoother(beep_absolute)
    avg_power = bottleneck.nanmean(beep_smoothed)
    threshold = 1.1 * avg_power
    beep_binary = beep_smoothed > threshold
    return Pulses(numpy.array(handlebars.rle(beep_binary)), compressed_iq_reading.ulid)


def pulses_to_deciles(pulses: Pulses) -> typing.Optional[Deciles]:
    """ takes an array of run-length encoded pulses, finds centroids for 'long' and 'short' """
    values = numpy.unique(pulses.sequence.T[1])
    deciles = {}
    if len(pulses.sequence) < 10:
        return deciles
    for value in values:
        counts = numpy.sort(pulses.sequence[numpy.where(pulses.sequence.T[1] == value)].T[0])
        tenth = len(counts) // 10
        if not tenth:
            return deciles
        short_decile = int(numpy.mean(counts[1 * tenth:2 * tenth]))
        long_decile = int(numpy.mean(counts[8 * tenth:9 * tenth]))
        deciles[value] = short_decile, long_decile
    logger.debug(f'using deciles {deciles}')
    return deciles


def pulses_to_breaks(pulses: Pulses, deciles: Deciles) -> typing.List[int]:
    """ with pulse array and definition of 'long' and 'short' pulses, find position of repeated packets """
    # heuristic - cutoff duration for a break between subsequent packets is at least 9x
    #  the smallest of the 'long' and 'short' pulses
    if 0 not in deciles.keys() or 1 not in deciles.keys():
        return []
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
    if not deciles:
        return
    breaks = pulses_to_breaks(pulses, deciles)
    if len(breaks) == 0:
        return
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
            yield Packet(bits, errors, deciles, pulses.sequence[x:y], pulses.ulid)


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
    ulid: str


def depacketize(packet: Packet, bitwidths: typing.List[int], output_class: dataclasses.dataclass):
    """ accept a Packet, list of field bitwidths, and output datatype. do the segmenting and binary conversion. """
    if bitwidths[0] != 0:
        bitwidths = [0] + bitwidths
    field_positions = [x for x in itertools.accumulate(bitwidths)]
    results = [handlebars.debinary(packet.bits[x:y]) for x, y in zip(field_positions, field_positions[1:])]
    return output_class(*results)


def silver_sensor_decoder(packet: Packet) -> typing.Optional[Sample]:
    """ hardware specific decoding function for the two types of silver sensors """
    success = False
    if packet.errors:
        return
    if len(packet.bits) == 42:
        n = depacketize(packet, [2, 8, 2, 2, 4, 4, 4, 4, 4, 1, 1, 6], SilverSensor_40b)
        if n.uid == 255:
            return
        temp = 16**2 * n.temp2 + 16 * n.temp1 + n.temp0
        humidity = 16 * n.rh1 + n.rh0
        if temp > 1000:
            temp %= 1000
            temp += 100
        temp = ((temp / 10) - 32) * 5/9
        success = True
    elif len(packet.bits) == 36:
        n = depacketize(packet, [4, 8, 2, 1, 1, 12, 8], SilverSensor_36b)
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
        return Sample(n.uid+1024+n.channel, temp, humidity, n.channel, n.low_battery, n.button_pushed, packet.ulid)


def pulses_to_sample(pulses: Pulses) -> Sample:
    """ high-level function accepting RLE'd pulses and returning a sample from a silver sensor if present """
    packet_possibilities = []
    for packet in demodulator(pulses):
        logger.debug(f'got: {len(packet.bits)} {printer(packet.bits)}')
        packet_possibilities += [silver_sensor_decoder(packet)]
    for possibility in packet_possibilities:
        if packet_possibilities.count(possibility) > 1:
            return possibility


async def block_to_sample() -> typing.Awaitable[None]:
    """ worker function which:
     * waits for blocks of analog data in redis queue
     * attempts to demodulate to pulses
     * attempts to decode to T&H.
     * broadcasts result / reports fail
     * updates expirations of analog data block """
    connection = await init_redis()
    async for reading in pseudosub(connection, 'transmissions'):
        ulid, timestamp, decoded = reading.ulid, reading.timestamp, None
        for filter_func in filters:
            pulses = block_to_pulses(reading, filter_func)
            if len(pulses.sequence) > 10:
                decoded = pulses_to_sample(pulses)
                if decoded:
                    break
            logger.info(f'decoded {decoded} with {filter_func.__name__}')
        if decoded:
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded.uid, tsd.degc, float(decoded.temperature)])
            await pseudopub(connection, ['datastore_channel', 'upstream_channel'], None, [timestamp, decoded.uid, tsd.rh, float(decoded.humidity)])
            await connection.sadd(f'{redis_prefix}_success_timestamps', timestamp)
            await connection.hincrby(f'{redis_prefix}_sensor_uuid_counts', decoded.uid, 1)
            await connection.hset(f'{redis_prefix}_sensor_uuid_timestamps', decoded.uid, timestamp)
            await connection.expire(f'{data_tag_label}{ulid}', 120)
        else:
            await connection.expire(f'{data_tag_label}{ulid}', 3600)
            await connection.sadd(f'{redis_prefix}_fail_timestamps', timestamp)


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


async def register_session_box(aiohttp_client_session):
    """ register node with backend. """
    human_name, uid = get_hardware_uid()
    logger.info('human name: %s' % human_name)
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    url = f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/register'
    status = await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, url, uid)
    logger.info(f'http successfully used to init uplink {status}')
    return (uid, packer, unpacker)

async def sample_to_upstream(loop=None):
    """ accumulates frames of latest values and periodically encrypts and sends to upstream via udp
     * sets up encrypted session over HTTP, ensures encryption key registered for node UID
     * listens on 'upstream_channel' """
    import socket
    import aiohttp
    heartbeat_url = f'{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/check'
    connection = await init_redis()
    await tick(connection)
    if loop == None:
        loop = asyncio.get_event_loop()
    aiohttp_client_session = aiohttp.ClientSession(loop=loop)
    uid, packer, unpacker = await register_session_box(aiohttp_client_session)
    udp_uplink_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    samples = {}
    last_sent = 0
    max_update_interval = 60
    max_intervals_till_check = 10
    intervals_till_next_verification = max_intervals_till_check
    async for reading in pseudosub(connection, 'upstream_channel'):
        timestamp = reading.timestamp
        reading = reading.value
        last_seen = float(await connection.hget(f'{redis_prefix}_sensor_uuid_timestamps', reading[1]) or '0')
        samples[reading[1] + 4096 * reading[2]] = reading
        now = time.time()
        if intervals_till_next_verification == 0:
            first_seen, most_recently_seen = await make_wrapped_http_request(aiohttp_client_session, packer, unpacker, heartbeat_url, uid)
            since_last_seen = int(now-most_recently_seen)
            if since_last_seen > 5 * max_update_interval:
                logger.error('backend reports 5x longer since last update than expected, terminating uplink')
                raise TimeoutError
            else:
                logger.info(f'backend confirms update received {int(now-most_recently_seen)}')
                intervals_till_next_verification = max_intervals_till_check
        if now > (last_sent + max_update_interval):
            update_samples = [x for x in samples.values()]
            logger.info(f'submitting {update_samples}')
            update_message = await packer(update_samples)
            udp_uplink_socket.sendto(update_message, (_constants.upstream_host, _constants.upstream_port))
            last_sent = time.time()
            intervals_till_next_verification -= 1


async def sample_to_datastore() -> typing.Awaitable[None]:
    """ logs samples in the local sqlite database """
    connection = await init_redis()
    datastore = tsd.TimeSeriesDatastore(db_name=data_dir + 'sproutwave_v1.db')
    sys.stderr.write(datastore.db_name + '\n')
    async for reading in pseudosub(connection, 'datastore_channel'):
        datastore.add_measurement(*reading.value, raw=True)
    return None


async def band_monitor(connection=None):
    """ periodically log a table of how many blocks we've successfully or unsuccessfully decoded for all frequencies """
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        now = time.time()
        good_stats = await connection.hgetall(f'{redis_prefix}_found_maybe_good_transmission_at_frequency')
        fail_stats = await connection.hgetall(f'{redis_prefix}_found_bad_transmission_at_frequency')
        channel_stats = dict(stats={}, timestamp=int(now))
        for freq, count in sorted(good_stats.items()):
            failed = fail_stats.get(freq)
            if failed == None:
                failed = 0
            else:
                failed = int(failed)
            channel_stats['stats'][int(freq)]=[int(count), failed]
        logger.info(json.dumps(channel_stats))
        await asyncio.sleep(60)


async def sensor_monitor(connection=None):
    """ periodically log a table of last seen and number times seen for all sensors """
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(handlebars.r1dx(20))
    while True:
        now = time.time()
        sensor_uuid_counts = await connection.hgetall(f'{redis_prefix}_sensor_uuid_counts')
        sensor_last_seen = await connection.hgetall(f'{redis_prefix}_sensor_uuid_timestamps')
        sensor_stats = dict(stats={}, timestamp=int(now))
        for uid, count in sorted(sensor_uuid_counts.items()):
            sensor_stats['stats'][int(uid)] = [int(count), int(now - float(sensor_last_seen[uid]))]
        logger.info(json.dumps(sensor_stats))
        await asyncio.sleep(60)


async def watchdog(connection=None, threshold=600, key='sproutwave_function_call_ticks'):
    """ run until it's been more than 600 seconds since all blocks ticked, then exit, signaling time for re-init """
    timeout = 600
    tick_cycle = 120
    if connection == None:
        connection = await init_redis()
    while True:
        now = time.time()
        ticks = await get_ticks(connection, key=key)
        process_stats = dict(stats={}, timestamp=int(now))
        for name, timestamp in ticks.items():
            process_stats['stats'][name.decode()] = int(now-timestamp)
        logger.info(json.dumps(process_stats))
        if any([(now - timestamp) > timeout for timestamp in ticks.values()]):
            await connection.delete(key)
            return
        await asyncio.sleep(tick_cycle - (time.time() - now))


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
