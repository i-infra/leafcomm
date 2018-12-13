import sys
sys.path.append('./')
import node_core
import cbor

import time

for fname in [fname for fname in sys.argv[1::] if '--' not in fname]:
    print('analysing', fname)
    f = open(fname, 'rb').read()
    start = time.time()
    info = cbor.loads(f)
    start = time.time()
    decoded = node_core.pulses_to_sample(node_core.block_to_pulses(info))
    end = time.time()
    print(end - start, decoded)
