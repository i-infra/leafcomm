from statistics import mode, mean, StatisticsError

import sys
import numpy as np
import json
import time
import itertools

printer = lambda xs: ''.join([{0: '░', 1: '█', 2: '╳'}[x] for x in xs])
debinary = lambda ba: sum([x*(2**i) for (i,x) in enumerate(reversed(ba))])

import beepshrink

import asyncio
import phase1

import tsd


ilen = lambda it: sum(1 for _ in it)
rle = lambda xs: ((ilen(gp), x) for x, gp in itertools.groupby(xs))
rld = lambda xs: itertools.chain.from_iterable(itertools.repeat(x, n) for n, x in xs)
# takes [(2, True), (2, True), (3, False)] -> [(4, True), (3, False)] without expansion
rerle = lambda xs: [(sum([i[0] for i in x[1]]), x[0]) for x in itertools.groupby(xs, lambda x: x[1])]

class PacketBase(object):
    def __init__(self, packet = [], errors = None, deciles = {}, raw = []):
        self.packet = packet
        self.errors = errors
        self.deciles = deciles
        self.raw = raw

def get_decile_durations(pulses):
    values = set([value for (width, value) in pulses])
    deciles = {}
    if len(pulses) < 10:
        return None
    for value in sorted(list(values)):
        counts = sorted([width for (width, x) in pulses if x == value])
        tenth = len(counts) // 10
        if not tenth:
            return None
        short_decile = int(mean(counts[1*tenth:2*tenth]))
        long_decile = int(mean(counts[8*tenth:9*tenth]))
        deciles[value] = (short_decile, long_decile)
    return deciles

def find_pulse_groups(pulses, deciles):
    # find segments of quiet that are 9x longer than the short period
    # this is naive, if a trivial pulse width encoding is used, any sequence of 9 or more short sequential silences will be read as a packet break
    breaks = [i[0] for i in enumerate(pulses) if (i[1][0] > min(deciles[0][0],deciles[1][0])  * 9) and (i[1][1] == False)]
    # find periodicity of the packets
    break_deltas = [y-x for (x,y) in zip(breaks, breaks[1::])]
    if len(break_deltas) < 2:
        return None
    # ignoring few-pulse packets, if you have more than three different fragment sizes, try to regularize
    elif len(set([bd for bd in break_deltas if bd > 3])) > 3:
        try:
            d_mode = mode(break_deltas)
        # if all values different, use mean as mode
        except StatisticsError:
            d_mode = round(mean(break_deltas))
        # determine expected periodicity of packet widths
        breaks2 = [x*d_mode for x in range(round(max(breaks) // d_mode))]
        if len(breaks2) < 2:
            return None
        # discard breaks more than 10% from expected position
        breaks = [x for x in breaks if True in [abs(x-y) < breaks2[1]//10 for y in breaks2]]
        # define packet pulses as the segment between breaks
    return breaks

def demodulator(pulses):
    packets = []
    # drop short (clearly erroneous, spurious) pulses
    pulses = rerle([x for x in pulses if x[0] > 2])
    deciles = get_decile_durations(pulses)
    if not deciles:
        return packets
    breaks = find_pulse_groups(pulses, deciles)
    if not breaks:
        return packets
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
                pb += [2]
        if len(pb) > 4:
            result = PacketBase(pb, errors, deciles, pulses[x:y])
            packets.append(result)
    return packets

def silver_sensor(packet):
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

async def main():
    connection = await phase1.get_connection()
    datastore = tsd.TimeSeriesDatastore()
    while True:
        timestamp = await connection.brpop(['eof_timestamps'], 360)
        timestamp = timestamp.value
        info = await connection.get(timestamp)
        start = time.time()
        if info == {}:
            pulses = []
        else:
            ba = beepshrink.decompress(**info)
            ba = np.absolute(ba) > np.mean(np.absolute(ba))
            pulses = [(w,v*1) for (w,v) in rle(ba)]
        res = None
        decoded = False
        if len(pulses) > 10:
            for packet in demodulator(pulses):
                print(printer(packet.packet))
                res = silver_sensor(packet)
                if (res is not None) and decoded is False:
                    uid = (res['channel']+1*1024)+res['uid']
                    #measurement = timestamp, sensor_uid, units, value)
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

        end = time.time()
        print(end-start)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
