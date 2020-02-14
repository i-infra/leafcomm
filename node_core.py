import asyncio
import atexit
import dataclasses
import itertools
import json
import os
import pathlib
import statistics
import sys
import time
import typing

import bottleneck
import numpy

import _constants
import ts_datastore
from node_comms import *
from node_controller import *

RTLSDR_SAMPLE_RATE = 256_000
L, H, E = 0, 1, 2
logger = get_logger(__name__, debug="--debug" in sys.argv)
global EXIT_NOW
EXIT_NOW = False
EXIT = b"EXIT"
EXIT_KEY = f"{redis_prefix}_exit"

FilterFunc = typing.Callable[[numpy.ndarray], numpy.ndarray]
Deciles = typing.Dict[int, typing.Tuple[int, int]]

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


def rerle(xs):
    return [
        (sum([i[0] for i in x[1]]), x[0]) for x in itertools.groupby(xs, lambda x: x[1])
    ]


def rle(xs):
    return [(len(list(gp)), x) for x, gp in itertools.groupby(xs)]


def rld(xs):
    return itertools.chain.from_iterable((itertools.repeat(x, n) for n, x in xs))


def flatten(l):
    return list(itertools.chain(*[[x] if type(x) not in [list] else x for x in l]))


def debinary(ba):
    return sum([x * 2 ** i for i, x in enumerate(reversed(ba))])


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
    return "".join([{L: "░", H: "█", E: "╳"}[x] for x in xs])


def brickwall(xs):
    return bottleneck.move_mean(xs, 32, 1)


try:
    import scipy
    from scipy.signal import butter, lfilter, freqz

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
        return lfilter(
            signal_filter_coefficients["b"], signal_filter_coefficients["a"], xs
        )

    filters = [brickwall, lowpass]
except:
    filters = [brickwall]


def packed_bytes_to_iq(samples: bytes) -> numpy.ndarray:
    bytes_numpy = numpy.ctypeslib.as_array(samples)
    iq = bytes_numpy.astype(numpy.float32).view(numpy.complex64)
    iq /= 127.5
    iq -= 1 + 1j
    return iq


def get_new_center():
    return min(433_800_000 + r1dx(10) * 20_000, 433940000)


async def analog_to_block():
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


def block_to_pulses(
    iq_reading: SerializedReading, smoother: FilterFunc = brickwall
) -> Pulses:
    """ takes a compressed block of analog samples, takes magnitude, amplitude-thresholds and returns pulses """
    beep_absolute = numpy.abs(iq_reading.value)
    beep_smoothed = smoother(beep_absolute)
    avg_power = bottleneck.nanmean(beep_smoothed)
    threshold = 1.1 * avg_power
    beep_binary = beep_smoothed > threshold
    return Pulses(numpy.array(rle(beep_binary)), iq_reading.ulid)


def pulses_to_deciles(pulses: Pulses) -> typing.Optional[Deciles]:
    """ takes an array of run-length encoded pulses, finds centroids for 'long' and 'short' """
    values = numpy.unique(pulses.sequence.T[1])
    deciles = {}
    if len(pulses.sequence) < 10:
        return deciles
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
        deciles[value] = sorted(
            [pulse_length for pulse_length, count in most_common_pulse_lengths[0:2]]
        )
    logger.debug(f"using deciles {deciles}")
    return deciles


def pulses_to_breaks(pulses: Pulses, deciles: Deciles) -> typing.List[int]:
    """ with pulse array and definition of 'long' and 'short' pulses, find position of repeated packets """
    if 0 not in deciles or 1 not in deciles:
        return []
    # heuristic - cutoff duration for a break between subsequent packets is at least 15x the shortest pulse
    cutoff = min(deciles[0][0], deciles[1][0]) * 15
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
    deciles = pulses_to_deciles(pulses)
    breaks = pulses_to_breaks(pulses, deciles)
    if len(breaks) == 0 or len(deciles) == 0:
        return
    for packet_start, packet_end in zip(breaks, breaks[1:]):
        pulse_group = pulses.sequence[packet_start + 1 : packet_end]
        symbols = []
        errors = []
        for pulse_width, chip_value in pulse_group:
            valid = False
            for value in deciles:
                for i, width in enumerate(deciles[value]):
                    if (
                        not valid
                        and chip_value == value
                        and abs(pulse_width - width) < width // 2
                    ):
                        symbols += [value] * (i + 1)
                        valid = True
            if not valid:
                errors += [(pulse_width, chip_value)]
                symbols.append(E)
        print(len(symbols), printer(symbols), errors)
        # heuristic - don't have packets with fewer than 10 symbols
        if E not in symbols and len(symbols) > 10:
            # NB. this is somewhat unique to the silver sensors in question
            # duration of low pulses determines bit value 100 -> 1; 10 -> 0
            yield Packet(symbols, errors, deciles, pulse_group, pulses.ulid)


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


@dataclasses.dataclass(unsafe_hash=True)
class DecodedSample:
    uid: int
    temperature: float
    humidity: float
    channel: int
    low_battery: bool
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


def depacketize(packet: Packet, bitwidths: typing.List[int], output_class=list):
    """ accept a Packet, list of field bitwidths, and output datatype. do the segmenting and binary conversion. """
    if bitwidths[0] != 0:
        bitwidths = [0] + bitwidths
    field_positions = [x for x in itertools.accumulate(bitwidths)]
    results = [
        debinary(packet.bits[x:y]) for x, y in zip(field_positions, field_positions[1:])
    ]
    return output_class(*results)


def silver_sensor_decoder(packet: Packet) -> typing.Optional[DecodedSample]:
    """ hardware specific decoding function for the most common type of silver sensor """
    success = False
    if packet.errors:
        return
    if len(packet.bits) in range(87, 96):
        packet.bits = [int(x[0] == 2) for x in rle(packet.bits) if x[1] == 0]
    if len(packet.bits) == 36:
        n = depacketize(packet, [4, 8, 2, 1, 1, 12, 8], SilverSensor_36b)
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
            n.low_battery,
            n.button_pushed,
            packet.ulid,
            ulid2.get_ulid_timestamp(packet.ulid),
        )


def etekcity_zap_decoder(packet: Packet) -> typing.Optional[DecodedCommand]:
    success = False
    if packet.errors:
        return
    if len(packet.bits) == 73:
        packet.bits = [
            int(width == 2) for (width, value) in rle(packet.bits) if value == 1
        ]
    if len(packet.bits) == 25:
        print(len(packet.bits), printer(packet.bits), packet.bits)
        if [1, 1] == packet.bits[-5:-3]:
            success = True
            state = True
        elif [1, 1] == packet.bits[-3:-1]:
            success = True
            state = False
        channel = [
            packet.bits[18:20],
            packet.bits[16:18],
            packet.bits[14:16],
            packet.bits[12:14],
            packet.bits[10:12],
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


async def handle_received_sample(connection, sample):
    if not isinstance(sample, DecodedSample):
        return
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


async def block_to_sample() -> typing.Awaitable[None]:
    """ worker function which:
     * waits for blocks of analog data in redis queue
     * attempts to demodulate to pulses
     * attempts to decode to T&H.
     * broadcasts result / reports fail
     * updates expirations of analog data block """
    connection = await init_redis()
    async for reading in pseudosub(connection, "transmissions"):
        sample = None
        for filter_func in filters:
            pulses = block_to_pulses(reading, filter_func)
            if len(pulses.sequence) > 10:
                for sample in pulses_to_sample(pulses):
                    logger.info(f"decoded {sample} with {filter_func.__name__}")
                    if sample:
                        await handle_received_sample(connection, sample)
                if sample:
                    break
        if not sample:
            await connection.expire(f"{data_tag_label}{reading.ulid}", 3600)
            await connection.sadd(f"{redis_prefix}_fail_timestamps", reading.timestamp)


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
    async for _ in pseudosub(connection, None, 60):
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
    async for _ in pseudosub(connection, None, tick_cycle):
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
        await connection.lpush(EXIT_KEY, EXIT)

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
        analog_to_block,
        block_to_sample,
        sample_to_datastore,
        sensor_monitor,
        run_alerts,
        run_controls,
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
