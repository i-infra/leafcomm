import sys
sys.path.append('./')
import phase1
import cbor

import time

for fname in sys.argv[1::]:
    print('analysing', fname)
    f = open(fname, 'rb').read()
    start = time.time()
    info = cbor.loads(f)

    pulses = phase1.get_pulses_from_info(info)
    print(pulses)
    packets = phase1.demodulator(pulses)

    for packet in packets:
        print(phase1.printer(packet.packet))
        print(packet.errors, packet.deciles)
        if packet.errors == []:
            break
    end = time.time()
    print(end-start)
