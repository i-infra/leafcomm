from statistics import mode, mean, StatisticsError

import numpy as np
import numexpr3 as ne3
import bottleneck as bn
import itertools as its
import typing
import beepshrink

import tsd
import phase1

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
    def butter_filter(data, cutoff, fs, btype='low', order=5):
        normalized_cutoff = cutoff / (0.5*fs)
        if btype == 'bandpass':
            normalized_cutoff = [normalized_cutoff // 10, normalized_cutoff]
        b, a = butter(order, normalized_cutoff, btype=btype, analog=False)
        b = b.astype('float32')
        a = a.astype('float32')
        y = lfilter(b, a, data)
        return y
    lowpass = lambda xs: butter_filter(xs, 2e3, 256e3, 'low', order=4)
    #bandpass = lambda xs: butter_filter(xs, 2e3, 256e3, 'bandpass', order=4)
except:
    scipy = None

def get_pulses_from_info(info, smoother=brickwall):
    beep_samples = beepshrink.decompress(**info)
    shape = beep_samples.shape
    beep_absolute = np.empty(shape, dtype='float32')
    ne3.evaluate('beep_absolute = abs(beep_samples)')
    beep_smoothed = smoother(beep_absolute)
    threshold = 1.1*bn.nanmean(beep_smoothed)
    beep_binary = np.empty(shape, dtype=bool)
    ne3.evaluate('beep_binary = beep_smoothed > threshold')
    pulses = rle(beep_binary)
    #pulses = rerle([x for x in rle(beep_binary) if x[0] > 2])
    return np.array(pulses)

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
    breaks = np.logical_and(pulses.T[0] > cutoff, pulses.T[1] == L).nonzero()[0]
    break_deltas = np.diff(breaks)
    if (len(breaks) > 50) or (len(breaks) < 5):
        return []
    elif len(np.unique(break_deltas[np.where(break_deltas > 3)])) > 3:
        try:
            d_mode = mode([bd for bd in break_deltas if bd > 3])
        except StatisticsError:
            d_mode = round(mean(break_deltas))
        expected_breaks = [x*d_mode for x in range(round(max(breaks) // d_mode))]
        if len(expected_breaks) < 2:
            return []
        tolerance = d_mode//10
        breaks = [x for (x,bd) in zip(breaks, break_deltas) if (True in [abs(x-y) < tolerance for y in expected_breaks]) or (bd == d_mode) or (bd < 5)]
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
            fields = [x for x in its.accumulate(fields)]
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
    return None


def decode_pulses(pulses): # -> {}
    if len(pulses) > 10:
        for packet in demodulator(pulses):
            print(printer(packet.packet))
            res = silver_sensor(packet)
            if (res is not None):
                res['uid'] = (res['channel']+1*1024)+res['uid']
                return res
            else:
                print('errors', packet.errors)
                print('deciles', packet.deciles)
    return {}

def try_decode(info, timestamp = 0): # -> {}
    if scipy is not None:
        filters = [brickwall, lowpass]
    else:
        filters = [brickwall]
    for filter_func in filters:
        pulses = get_pulses_from_info(info, filter_func)
        decoded = decode_pulses(pulses)
        if decoded != {}:
            return decoded
    return {}

async def main():
    connection = await phase1.get_connection()
    datastore = tsd.TimeSeriesDatastore()
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
            datastore.add_measurement(timestamp, decoded['uid'], 'degc', decoded['temperature'])
            datastore.add_measurement(timestamp, decoded['uid'], 'rh', decoded['humidity'])
            await connection.delete([timestamp])

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
