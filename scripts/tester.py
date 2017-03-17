import sys
sys.path.append('./')
import packetizer, cbor
import numpy as np

import time

for fname in sys.argv[1::]:
    print('analysing', fname)
    f = open(fname, 'rb').read()
    start = time.time()
    info = cbor.loads(f)

    pulses = packetizer.get_pulses_from_info(info)
    packets = packetizer.demodulator(pulses)

    for packet in packets:
        print(packetizer.printer(packet.packet))
        print(packet.errors, packet.deciles)
        if packet.errors == []:
            break
    end = time.time()
    print(end-start)
