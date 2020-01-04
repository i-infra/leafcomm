import sys
import time

import cbor

sys.path.append("./")


from node_core import *

for fname in [fname for fname in sys.argv[1::] if "--" not in fname]:
    print("analysing", fname)
    f = open(fname, "rb").read()
    start = time.time()
    if f[0] == 0: # raw c64
        uid = fname.split('/')[-1].lstrip('_').rstrip('.beep')
        llen = len(f) // 8
        iq = numpy.ndarray(llen, dtype='float32',buffer=f).view(dtype='complex64')
        reading = SerializedReading(uid, cbor.dumps(compress(iq)))
    elif f[0] == 0xa3: # cbor encoded compressed numpy array
        uid = ulid2.generate_ulid_as_base32()
        reading = SerializedReading(uid, f)
    for f_ in [brickwall, lowpass]:
        decoded = pulses_to_sample(block_to_pulses(reading, f_))
        end = time.time()
        d = None
        for d in decoded:
            print(end - start, f_, d)
        if d:
            break
