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
    start = time.time()
    decoded = phase1.try_decode(info)
    end = time.time()
    print(end-start, decoded)
