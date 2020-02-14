import sys
import time

import cbor

sys.path.append("./")


from node_core import *

for fname in [fname for fname in sys.argv[1::] if "--" not in fname]:
    print("analysing", fname)
    f = open(fname, "rb").read()
    start = time.time()
    if f[0] in [0, 0x80]:  # raw c64
        uid = fname.split("/")[-1].lstrip("_").rstrip(".beep")
        llen = len(f) // 16
        iq = numpy.ndarray(llen, dtype="complex64", buffer=f)
        reading = SerializedReading(uid, value=iq)
    elif f[0] == 0xA3:  # cbor encoded compressed numpy array
        uid = ulid2.generate_ulid_as_base32()
        reading = SerializedReading(uid, decompress(**cbor.loads(f)))
    for f_ in [brickwall, lowpass]:
        pulses = block_to_pulses(reading, f_)
        packet_possibilities = []
        for packet in list(demodulator(pulses)):
            for decoder in [silver_sensor_decoder, etekcity_zap_decoder]:
                packet_possibilities += [decoder(packet)]
        for possibility in set(packet_possibilities):
            print(possibility)
