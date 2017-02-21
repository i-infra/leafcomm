import beepshrink, packetizer, cbor
import numpy as np
import sys

# accepts a '.beep' filename as the argument, attempts to run through early stage packetization / framing

f = open(sys.argv[1], 'rb').read()
g = cbor.loads(f)
h = beepshrink.decompress(**g)
a = np.absolute(h)
print(np.max(a), np.mean(a))
pulses = [x for x in packetizer.rle(a > np.mean(a))]
packets = packetizer.demodulator(pulses)
for packet in packets:
    print(packetizer.printer(packet.packet))
    print(packet.errors, packet.deciles)
