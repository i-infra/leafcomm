import beepshrink, packetizer, cbor
import numpy as np
import sys

import time

for fname in sys.argv[1::]:
    print('analysing', fname)
    f = open(fname, 'rb').read()
    info = cbor.loads(f)

    start = time.time()
    pulses = packetizer.get_pulses_from_info(info)
    end = time.time()
    print(end-start)
    packets = packetizer.demodulator(pulses)

    for packet in packets:
        print(packetizer.printer(packet.packet))
        print(packet.errors, packet.deciles)
        if packet.errors == []:
            break
