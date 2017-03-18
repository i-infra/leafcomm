import sys
sys.path.append('./')
import packetizer, cbor

import time

for fname in sys.argv[1::]:
    print('analysing', fname)
    f = open(fname, 'rb').read()
    info = cbor.loads(f)
    start = time.time()
    decoded = packetizer.try_decode(info)
    end = time.time()
    print(end-start, decoded)
