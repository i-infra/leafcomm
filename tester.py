import beepshrink, packetizer, cbor
import numpy as np
import sys

import time

f = open(sys.argv[1], 'rb').read()
g = cbor.loads(f)
h = beepshrink.decompress(**g)

start = time.time()
pulses = packetizer.get_pulses_from_analog(g)
end = time.time()
print(end-start)
packets = packetizer.demodulator(pulses)

for packet in packets:
    print(packetizer.printer(packet.packet))
    print(packet.errors, packet.deciles)
    if packet.errors == []:
        break
