import asyncio
import atexit
import dataclasses
import json
import os
import pathlib
import statistics
import sys
import time
import typing

import _constants
import bottleneck
import node_controller
import numpy
import scipy
import ts_datastore
import fir_coef
from binary_ops import *
from node_comms import *
from scipy.signal import butter, freqz, lfilter

RTLSDR_SAMPLE_RATE = 256_000
L, H = 0, 1
logger = get_logger(__name__, debug="--debug" in sys.argv)
global EXIT_NOW
EXIT_NOW = False
EXIT_VALUE = b"EXIT"
EXIT_KEY = f"{redis_prefix}_exit"

FilterFunc = typing.Callable[[numpy.ndarray], numpy.ndarray]
length_clusters = typing.Dict[int, typing.Tuple[int, int]]

krng = open("/dev/urandom", "rb")


def r1dx(x=20):
    """ returns a random, fair integer from 1 to X as if rolling a dice with the specified number of sides """
    max_r = 256
    assert x <= max_r
    while True:
        # get one byte, take as int on [1,256]
        r = int.from_bytes(krng.read(1), "little") + 1
        # if byte is less than the max factor of 'x' on the interval max_r, return r%x+1
        if r < (max_r - (max_r % x) + 1):
            return (r % x) + 1


@dataclasses.dataclass
class Packet:
    bits: list
    length_clusters: dict
    raw: list
    ulid: str


@dataclasses.dataclass
class Pulses:
    sequence: list
    ulid: str


def printer(xs):
    return "".join([{L: "░", H: "█"}[x] if x in (L,H) else "╳" for x in xs])


def brickwall(xs):
    return bottleneck.move_mean(xs, 32, 1)


def build_butter_filter(cutoff, fs, btype="low", order=5):
    normalized_cutoff = cutoff / (0.5 * fs)
    if btype == "bandpass":
        normalized_cutoff = [normalized_cutoff // 10, normalized_cutoff]
    b, a = butter(order, normalized_cutoff, btype=btype, analog=False)
    b = b.astype("float32")
    a = a.astype("float32")
    return b, a


signal_filter_coefficients = dict(
    zip(("b", "a"), build_butter_filter(2000, RTLSDR_SAMPLE_RATE, order=4))
)


def lowpass(xs):
    return lfilter(signal_filter_coefficients["b"], signal_filter_coefficients["a"], xs)


def _next_regular(target):
    """
    Find the next regular number greater than or equal to target.
    Regular numbers are composites of the prime factors 2, 3, and 5.
    Also known as 5-smooth numbers or Hamming numbers, these are the optimal
    size for inputs to FFTPACK.
    Target must be a positive integer.
    """
    if target <= 6:
        return target

    # Quickly check if it's already a power of 2
    if not (target & (target - 1)):
        return target

    match = float("inf")  # Anything found will be smaller
    p5 = 1
    while p5 < target:
        p35 = p5
        while p35 < target:
            # Ceiling integer division, avoiding conversion to float
            # (quotient = ceil(target / p35))
            quotient = -(-target // p35)

            # Quickly find next power of 2 >= quotient
            try:
                p2 = 2 ** ((quotient - 1).bit_length())
            except AttributeError:
                # Fallback for Python <2.7
                p2 = 2 ** (len(bin(quotient - 1)) - 2)

            N = p2 * p35
            if N == target:
                return N
            elif N < match:
                match = N
            p35 *= 3
            if p35 == target:
                return p35
        if p35 < match:
            match = p35
        p5 *= 5
        if p5 == target:
            return p5
    if p5 < match:
        match = p5
    return match


def bandpass_separator(xs):
    n_taps = 127
    logger = get_logger()
    target_length = _next_regular(len(xs))
    fft_length = target_length + n_taps - 1
    logger.info(f"using target length: {target_length}")
    z = numpy.fft.fft(xs, fft_length)
    zmag = numpy.abs(z)
    zmax = max(z)
    zfreqs = numpy.fft.fftfreq(len(z), 1 / RTLSDR_SAMPLE_RATE)
    zmag = bottleneck.move_mean(zmag, 128)
    zmag[0:128] = 0
    zmax = int(max(zmag))
    peaks, peak_metadata = scipy.signal.find_peaks(
        zmag, prominence=zmax / 5, distance=8000
    )
    peaks += 128
    centroids = sorted(zfreqs[peaks])
    logger.info(f"found centers: {centroids}")
    if len(centroids) == 1:
        return [xs]
    elif len(centroids) < 4:
        designed_fir_filters = [
            fir_coef.firwin(
                numtaps=n_taps,
                fs=RTLSDR_SAMPLE_RATE,
                window="hamming",
                cutoff=(fc - 8000, fc + 8000),
                pass_zero="bandpass",
            )
            for fc in centroids
        ]
        filtered = [
            numpy.fft.ifft(z * numpy.fft.fft(fir_coefs, fft_length))
            for fir_coefs in designed_fir_filters
        ]
        return filtered


am_filters = [brickwall, lowpass]
rf_filters = [bandpass_separator]


def packed_bytes_to_iq(samples: bytes) -> numpy.ndarray:
    bytes_numpy = numpy.ctypeslib.as_array(samples)
    iq = bytes_numpy.astype(numpy.float32).view(numpy.complex64)
    iq /= 127.5
    iq -= 1 + 1j
    return iq


def get_new_center():
    return min(433_800_000 + r1dx(10) * 20_000, 433940000)


async def rtlsdr_threshold_and_accumulate():
    from rtlsdr import rtlsdraio

    logger = get_logger()
    sdr = rtlsdraio.RtlSdrAio()
    sdr.rs, sdr.fc, sdr.gain = RTLSDR_SAMPLE_RATE, 433900000.0, 9
    connection = await init_redis()
    bytes_per_block = 512 * 64
    samples_per_block = bytes_per_block // 2
    center_frequency = sdr.fc
    max_blocks_per_sample = 64
    block_power_history = 256
    complex_samples = numpy.zeros(
        samples_per_block * max_blocks_per_sample, dtype=numpy.complex64
    )
    int8_samples = numpy.zeros(
        bytes_per_block * max_blocks_per_sample, dtype=numpy.int8
    )
    block_powers = numpy.zeros(max_blocks_per_sample * 4, dtype=numpy.float32)
    block_index = 0
    running_average_block_power = 0
    power_history_index = 0
    start_of_transmission_timestamp = 0
    one_stddev = 100
    async for byte_samples in sdr.stream(bytes_per_block, format="bytes"):
        accumulated = False
        if await connection.llen(EXIT_KEY):
            break
        samp_index = block_index * samples_per_block
        complex_samples[
            samp_index : samp_index + samples_per_block
        ] = packed_bytes_to_iq(byte_samples)
        block_powers[power_history_index] = numpy.sum(
            numpy.abs(complex_samples[samp_index : samp_index + samples_per_block])
        )
        running_average_block_power = bottleneck.nanmean(block_powers)
        # if current block power is greater than 10% of 1stddev + avg block power
        if (
            block_powers[power_history_index]
            >= running_average_block_power + one_stddev / 10
        ):
            if block_index == 0:
                start_of_transmission_timestamp = time.time()
            block_index += 1
        else:
            if block_index > 1:
                end_of_transmission_timestamp = time.time()
            if block_index > 9:
                accumulated = True
                one_stddev = numpy.std(block_powers)
                ulid = await pseudopub(
                    connection,
                    "transmissions",
                    timestamp=start_of_transmission_timestamp,
                    reading=complex_samples[: samp_index + samples_per_block],
                    metadata={"center_frequency": center_frequency},
                )
        if accumulated:
            logger.info(
                f"ACCUMULATED: {(ulid, block_index, power_history_index, running_average_block_power, block_powers[power_history_index], one_stddev,)}"
            )
            block_index = 0
        power_history_index += 1
        power_history_index %= block_power_history
        block_index %= max_blocks_per_sample
        if power_history_index == 0:
            center_frequency = get_new_center()
            sdr.set_center_freq(center_frequency)
    await sdr.stop()
    sdr.close()


def pulses_to_length_clusters(pulses: Pulses) -> typing.Optional[length_clusters]:
    """ takes an array of run-length encoded pulses, finds centroids for 'long' and 'short' """
    values = numpy.unique(pulses.sequence.T[1])
    length_clusters = {}
    logger = get_logger()
    if len(pulses.sequence) < 10:
        return length_clusters
    for value in values:
        counts = numpy.sort(
            pulses.sequence[numpy.where(pulses.sequence.T[1] == value)].T[0]
        )
        grouped_counts = {}
        replacements = {}
        for occurrences, pulse_length in sorted(
            rle(counts), key=lambda x: x[0], reverse=True
        ):
            if pulse_length in grouped_counts:
                grouped_counts[pulse_length] += occurrences
            else:
                for already_counted in grouped_counts:
                    # if within 30%, snap to existing pulse width
                    if abs(already_counted - pulse_length) / already_counted < 0.30:
                        grouped_counts[already_counted] += occurrences
                        replacements[pulse_length] = already_counted
                        break
                if pulse_length not in replacements:
                    grouped_counts[pulse_length] = occurrences
        grouped_counts = {
            length: count for length, count in grouped_counts.items() if count > 1
        }
        most_common_pulse_lengths = sorted(
            grouped_counts.items(), key=lambda x: x[1], reverse=True
        )
        logger.debug(f"most_common_pulse_lengths: {most_common_pulse_lengths}")
        length_clusters[value] = sorted(
            [pulse_length for pulse_length, count in most_common_pulse_lengths[0:2]]
        )
    logger.debug(f"using length_clusters {length_clusters}")
    return length_clusters


def pulses_to_breaks(
    pulses: Pulses, length_clusters: length_clusters
) -> typing.List[int]:
    """ with pulse array and definition of 'long' and 'short' pulses, find position of repeated packets """
    logger = get_logger()
    if 0 not in length_clusters or 1 not in length_clusters:
        return []
    # heuristic - cutoff duration for a break between subsequent packets is at least 15x the shortest pulse
    cutoff = min(length_clusters[0][0], length_clusters[1][0]) * 15
    breaks = numpy.logical_and(
        pulses.sequence.T[0] > cutoff, pulses.sequence.T[1] == L
    ).nonzero()[0]
    break_deltas = numpy.diff(breaks)
    # heuristic - if more than 3 different spacings between detected repeated packets, tighten up purposed breaks
    if len(breaks) > 100 or len(breaks) < 3:
        return []
    if len(numpy.unique(break_deltas[numpy.where(break_deltas > 3)])) > 3:
        try:
            d_mode = statistics.mode([bd for bd in break_deltas if bd > 3])
        except statistics.StatisticsError:
            d_mode = round(statistics.mean(break_deltas))
        expected_breaks = [x * d_mode for x in range(round(max(breaks) // d_mode))]
        if len(expected_breaks) < 2:
            return []
        tolerance = d_mode // 10
        breaks = [
            x
            for x, bd in zip(breaks, break_deltas)
            if any([abs(x - y) < tolerance for y in expected_breaks])
            or bd == d_mode
            or bd < 5
        ]
    logger.debug(f"using breaks {breaks}")
    return breaks


def demodulator(pulses: Pulses) -> typing.Iterable[Packet]:
    """ high level demodulator function accepting a pulse array and yielding 0 or more Packets """
    length_clusters = pulses_to_length_clusters(pulses)
    breaks = pulses_to_breaks(pulses, length_clusters)
    logger = get_logger()
    if len(breaks) == 0 or len(length_clusters) == 0:
        return
    for packet_start, packet_end in zip(breaks, breaks[1:]):
        pulse_group = pulses.sequence[packet_start + 1 : packet_end]
        symbols = []
        for pulse_width, chip_value in pulse_group:
            valid = False
            for value in length_clusters:
                for i, width in enumerate(length_clusters[value]):
                    if (
                        not valid
                        and chip_value == value
                        and abs(pulse_width - width) < width // 2
                    ):
                        symbols += [value] * (i + 1)
                        valid = True
            if not valid:
                if len(symbols) > 10:
                    yield Packet(symbols, length_clusters, pulse_group, pulses.ulid)
                symbols = []
        logger.info(f"{len(symbols)} {printer(symbols)}")
        if len(symbols) > 10:
            yield Packet(symbols, length_clusters, pulse_group, pulses.ulid)


@dataclasses.dataclass
class SilverSensor_36b:
    # "Prologue, FreeTec NC-7104, NC-7159-675 temperature sensor"
    model: int
    uid: int
    battery_ok: bool
    button_pushed: bool
    channel: int
    temp: int
    rh: int


@dataclasses.dataclass(unsafe_hash=True)
class DecodedSample:
    uid: int
    temperature: float
    humidity: float
    channel: int
    battery_ok: bool
    button_pushed: bool
    ulid: str
    timestamp: float


@dataclasses.dataclass(unsafe_hash=True)
class DecodedCommand:
    uid: int
    channel: int
    state: bool
    ulid: str
    timestamp: float


def silver_sensor_decoder(packet: Packet) -> typing.Optional[DecodedSample]:
    """ hardware specific decoding function for the most common type of silver sensor """
    success = False
    if len(packet.bits) in range(85, 230):
        packet.bits = [int(x[0] == 2) for x in rle(packet.bits) if x[1] == 0]
    if len(packet.bits) == 36:
        n = extract_fields_from_bits(
            packet.bits, [4, 8, 1, 1, 2, 12, 8], SilverSensor_36b
        )
        temp = n.temp
        if temp >= 3840:
            temp -= 4096
        temp /= 10
        humidity = n.rh
        if n.model == 5:
            success = True
    if success:
        return DecodedSample(
            n.uid + 1024 + n.channel,
            temp,
            humidity,
            n.channel,
            n.battery_ok,
            n.button_pushed,
            packet.ulid,
            ulid2.get_ulid_timestamp(packet.ulid),
        )


def etekcity_zap_decoder(packet: Packet) -> typing.Optional[DecodedCommand]:
    success = False
    if len(packet.bits) == 73:
        packet.bits = [
            int(width == 2) for (width, value) in rle(packet.bits) if value == 1
        ]
    if len(packet.bits) == 25:
        if [1, 1] == packet.bits[-5:-3]:
            success = True
            state = False
        elif [1, 1] == packet.bits[-3:-1]:
            success = True
            state = True
        channel = [
            packet.bits[18:20],
            packet.bits[16:18],
            packet.bits[14:16],
            packet.bits[12:14],
            packet.bits[10:12],
            [1, 1],
        ].index([1, 1])
        uid = debinary(packet.bits[1::2][:9])
    if success:
        return DecodedCommand(
            state=state,
            ulid=packet.ulid,
            channel=channel,
            uid=uid,
            timestamp=ulid2.get_ulid_timestamp(packet.ulid),
        )


def pulses_to_sample(pulses: Pulses) -> DecodedSample:
    """ high-level function accepting RLE'd pulses and returning a sample from a silver sensor if present """
    packet_possibilities = []
    for packet in demodulator(pulses):
        for decoder in [silver_sensor_decoder, etekcity_zap_decoder]:
            packet_possibilities += [decoder(packet)]
    for possibility in set(packet_possibilities):
        if packet_possibilities.count(possibility) > 1:
            yield possibility


async def publish_decoded_sample(connection, sample):
    frames = [
        [sample.timestamp, sample.uid, ts_datastore.degc, float(sample.temperature)],
        [sample.timestamp, sample.uid, ts_datastore.rh, float(sample.humidity)],
    ]
    for frame in frames:
        await pseudopub(
            connection,
            ["datastore_channel", "upstream_channel", "feedback_channel"],
            sample.timestamp,
            frame,
        )

    await connection.sadd(f"{redis_prefix}_success_timestamps", sample.timestamp)
    await connection.hincrby(f"{redis_prefix}_sensor_uuid_counts", sample.uid, 1)
    await connection.hset(
        f"{redis_prefix}_sensor_uuid_timestamps", sample.uid, sample.timestamp
    )
    await connection.expire(f"{data_tag_label}{sample.ulid}", 120)


async def process_iq_readings() -> typing.Awaitable[None]:
    """ worker function which:
     * waits for blocks of analog data in redis queue
     * attempts to demodulate to pulses
     * attempts to decode to T&H.
     * broadcasts result / reports fail
     * updates expirations of analog data block """
    connection = await init_redis()
    async for iq_reading in pseudosub(connection, "transmissions"):
        if (iq_reading == None) or isinstance(iq_reading.value, type(None)):
            continue
        decoded = process_iq_reading(iq_reading)
        for sample in decoded:
            if isinstance(sample, DecodedSample):
                await publish_decoded_sample(connection, sample)
        else:
            await connection.expire(f"{data_tag_label}{iq_reading.ulid}", 3600)
            await connection.sadd(
                f"{redis_prefix}_fail_timestamps", iq_reading.timestamp
            )


def process_iq_reading(iq_reading):
    decoded = []
    for rf_filter in [None] + rf_filters:
        rf_filter_name = rf_filter.__name__ if rf_filter != None else "passthrough"
        for am_filter in am_filters:
            am_filter_name = am_filter.__name__ if am_filter != None else "passthrough"
            if rf_filter:
                rfs = rf_filter(iq_reading.value)
                if not rfs or len(rfs) == 0:
                    break
            else:
                rfs = [iq_reading.value]
            for filtered_rf_beep in rfs:
                beep_amplitude = numpy.abs(filtered_rf_beep)
                if am_filter:
                    beep_smoothed = am_filter(beep_amplitude)
                else:
                    beep_smoothed = beep_amplitude
                avg_power = bottleneck.nanmean(beep_smoothed)
                threshold = 1.1 * avg_power
                beep_binary = beep_smoothed > threshold
                pulses = Pulses(numpy.array(rle(beep_binary)), iq_reading.ulid)
                # NB: Heuristic upper bound: 2 * 6 repeats * 100-odd pulses a transmit
                if len(pulses.sequence) > 10 and len(pulses.sequence) < 1800:
                    decoded += [
                        sample for sample in pulses_to_sample(pulses) if sample != None
                    ]
                    logger.info(
                        f"decoded {decoded} with {rf_filter_name}, {am_filter_name} from {iq_reading.ulid}"
                    )
            if decoded != []:
                break
        if decoded != []:
            break
    return decoded


def get_hardware_uid() -> (str, bytes):
    """ get human name and hash of SD card CID """
    import nacl.hash
    import nacl.encoding
    import glob
    import colourful_vegetables

    raw_sd_card_id = bytes.fromhex(
        open(glob.glob("/sys/block/mmcblk*/device/cid")[0], "r").read()
    )
    raw_hashed_id = nacl.hash.sha256(raw_sd_card_id, encoder=nacl.encoding.RawEncoder)
    colour = colourful_vegetables.colours[raw_hashed_id[0] >> 4]
    veggy = colourful_vegetables.vegetables[raw_hashed_id[0] & 15]
    node_id = AbbreviatedBase32Encoder.encode(raw_hashed_id)
    human_name = "%s %s" % (colour, veggy)
    return (human_name, node_id)


async def wait_or_exit(connection, tick_time):
    res = await connection.blpop(EXIT_KEY, timeout=tick_time)
    if res:
        await connection.lpush(EXIT_KEY, res[1])
        return res[1]


async def register_session_box(aiohttp_client_session):
    """ register node with backend. """
    human_name, uid = get_hardware_uid()
    logger.info("human name: %s" % human_name)
    packer, unpacker = await get_packer_unpacker(_constants.upstream_pubkey_bytes)
    url = f"{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/register"
    status = await make_wrapped_http_request(
        aiohttp_client_session, packer, unpacker, url, uid
    )
    logger.info(f"http successfully used to init uplink {status}")
    return (uid, packer, unpacker)


async def sample_to_upstream(loop=None):
    """ accumulates frames of latest values and periodically encrypts and sends to upstream via udp
     * sets up encrypted session over HTTP, ensures encryption key registered for node UID
     * listens on 'upstream_channel' """
    import socket
    import aiohttp

    logger = get_logger()

    heartbeat_url = f"{_constants.upstream_protocol}://{_constants.upstream_host}:{_constants.upstream_port}/check"
    connection = await init_redis()
    await connection.hset(f"{redis_prefix}_f_ticks", get_function_name(), time.time())
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
    async for reading in pseudosub(connection, "upstream_channel"):
        if reading != None and reading.value != None:
            timestamp = reading.timestamp
            reading = reading.value
            last_seen = float(
                await connection.hget(
                    f"{redis_prefix}_sensor_uuid_timestamps", reading[1]
                )
                or 0
            )
            samples[reading[1] + 4096 * reading[2]] = reading
            now = time.time()
            if intervals_till_next_verification == 0:
                first_seen, most_recently_seen = await make_wrapped_http_request(
                    aiohttp_client_session, packer, unpacker, heartbeat_url, uid
                )
                since_last_seen = int(now - most_recently_seen)
                if since_last_seen > 5 * max_update_interval:
                    logger.error(
                        "backend reports 5x longer since last update than expected, terminating uplink"
                    )
                    raise TimeoutError
                else:
                    logger.info(
                        f"backend confirms update received {int(now-most_recently_seen)}"
                    )
                    intervals_till_next_verification = max_intervals_till_check
        if now > (last_sent + max_update_interval):
            update_samples = [x for x in samples.values()]
            logger.info(f"submitting {update_samples}")
            update_message = await packer(update_samples)
            udp_uplink_socket.sendto(
                update_message, (_constants.upstream_host, _constants.upstream_port)
            )
            last_sent = time.time()
            intervals_till_next_verification -= 1
    await aiohttp_client_session.close()


async def sample_to_datastore() -> typing.Awaitable[None]:
    """ logs samples in the local sqlite database """
    connection = await init_redis()
    datastore = ts_datastore.TimeSeriesDatastore(db_name=data_dir + "sproutwave_v1.db")
    sys.stderr.write(datastore.db_name + "\n")
    async for reading in pseudosub(connection, "datastore_channel"):
        datastore.add_measurement(*reading.value, raw=True)


async def sensor_monitor(connection=None, tick_time=60):
    """ periodically log a table of last seen and number times seen for all sensors """
    if connection == None:
        connection = await init_redis()
    await asyncio.sleep(r1dx(20))
    logger = get_logger()
    async for _ in tick_on_schedule(connection, 60):
        now = time.time()
        sensor_uuid_counts = await connection.hgetall(
            f"{redis_prefix}_sensor_uuid_counts"
        )
        sensor_last_seen = await connection.hgetall(
            f"{redis_prefix}_sensor_uuid_timestamps"
        )
        sensor_stats = dict(stats={}, timestamp=int(now))
        for uid, count in sorted(sensor_uuid_counts.items()):
            sensor_stats["stats"][int(uid)] = [
                int(count),
                int(now - float(sensor_last_seen[uid])),
            ]
        logger.info(json.dumps(sensor_stats))


async def watchdog(connection=None, threshold=600, key="sproutwave_f_ticks"):
    """ run until it's been more than 600 seconds since all blocks ticked, then exit, signaling time for re-init """
    timeout = 600
    tick_cycle = 120
    if connection == None:
        connection = await init_redis()
    # clear f_ticks
    await connection.delete(key)
    logger = get_logger()
    async for _ in tick_on_schedule(connection, tick_cycle):
        now = time.time()
        ticks = await get_ticks(connection, key=key)
        process_stats = dict(stats={}, timestamp=int(now))
        for name, timestamp in ticks.items():
            process_stats["stats"][name.decode()] = int(now - timestamp)
        logger.info(json.dumps(process_stats))
        if any([(now - timestamp) > timeout for timestamp in ticks.values()]):
            await connection.delete(key)
            raise TimeoutError("Watchdog detected timeout.")


def time_to_exit(signalnum, frame):
    global EXIT_NOW

    async def indicate_exit():
        connection = await init_redis()
        await connection.lpush(EXIT_KEY, EXIT_VALUE)
        await connection.lpush(EXIT_KEY, EXIT_VALUE)
        await connection.lpush(EXIT_KEY, EXIT_VALUE)

    multi_spawner(indicate_exit).join()
    EXIT_NOW = True


def cleanup_exit():
    global EXIT_NOW

    async def cleanup_exit():
        connection = await init_redis()
        return await connection.delete(EXIT_KEY)

    if EXIT_NOW:
        multi_spawner(cleanup_exit).join()


def main():
    pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGINT, time_to_exit)
    atexit.register(cleanup_exit)
    redis_server_process = start_redis_server(
        redis_socket_path=f"{data_dir}sproutwave.sock"
    )
    # TODO: replace time.sleep with polling for redis, or have start_redis_server do so...
    time.sleep(5)
    funcs = (
        rtlsdr_threshold_and_accumulate,
        process_iq_readings,
        sample_to_datastore,
        sensor_monitor,
        node_controller.calculate_controls,
        node_controller.command_controls,
    )
    proc_mapping = {}
    while not EXIT_NOW:
        for func in funcs:
            name = func.__name__
            logger.info("(re)starting %s" % name)
            if name in proc_mapping.keys():
                proc_mapping[name].terminate()
            proc_mapping[name] = multi_spawner(func)
        time.sleep(30)
        multi_spawner(watchdog).join()


if __name__ == "__main__":
    main()
