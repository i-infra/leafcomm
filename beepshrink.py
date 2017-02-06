import blosc
import numpy as np
#  *** lz4     , 1, bitshuffle ***  0.011 s (124.73 MB/s) / 0.011 s (124.08 MB/s)        Compr. ratio:   8.0x
#  *** lz4     , 3, bitshuffle ***  0.013 s (103.54 MB/s) / 0.011 s (125.50 MB/s)        Compr. ratio:   8.1x
#  *** lz4     , 4, bitshuffle ***  0.014 s (98.39 MB/s) / 0.015 s (90.88 MB/s)  Compr. ratio:   8.4x
#  *** lz4     , 7, bitshuffle ***  0.018 s (77.10 MB/s) / 0.044 s (31.01 MB/s)  Compr. ratio:   8.8x
#  *** lz4     , 9, bitshuffle ***  0.026 s (52.26 MB/s) / 0.065 s (21.13 MB/s)  Compr. ratio:   8.4x


# tuned constants for beep data on ARM
clevel = 1
shuffle = blosc.BITSHUFFLE
cname = 'lz4'

compress = lambda in_: (in_.size, in_.dtype, blosc.compress_ptr(in_.__array_interface__['data'][0], in_.size, in_.dtype.itemsize, clevel=clevel, shuffle=shuffle, cname=cname))

def decompress(size, dtype, compressed):
    out = np.empty(size, dtype)
    blosc.decompress_ptr(compressed, out.__array_interface__['data'][0])
    return out

#(size, dtype, compressed) = compress(in_)
#decompressed = decompress(size, dtype, compressed)
