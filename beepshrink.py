import blosc
import numpy as np

compress = lambda in_: (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=1, shuffle=blosc.BITSHUFFLE, cname='lz4'))

def decompress(size, dtype, data):
    out = np.empty(size, dtype)
    blosc.decompress_ptr(data, out.__array_interface__['data'][0])
    return out
