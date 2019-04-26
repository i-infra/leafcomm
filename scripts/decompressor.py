import sys
import time

import cbor
import node_core

sys.path.append("./")


for fname in sys.argv[1::]:
    if fname == "-":
        f = sys.stdin.buffer.read()
    else:
        f = open(fname, "rb").read()
    start = time.time()
    info = cbor.loads(f)
    start = time.time()
    decoded = node_core.decompress(**info)
    end = time.time()
    open("/dev/stdout", "wb").write(decoded.tobytes())
