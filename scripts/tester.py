import sys
import time

import cbor
import node_core

sys.path.append("./")


for fname in [fname for fname in sys.argv[1::] if "--" not in fname]:
    print("analysing", fname)
    f = open(fname, "rb").read()
    start = time.time()
    decoded = node_core.pulses_to_sample(node_core.block_to_pulses(cbor.loads(f)))
    end = time.time()
    print(end - start, decoded)
