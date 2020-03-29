import numpy

masters = {10: b"\x01U\x03", 203: b"\x01\x15\x03"}

# >>> x = numpy.array(etek_codes.codes_0203)
# >>> b"".join(int(x).to_bytes(1, 'little') for x in numpy.packbits((x[0] & x[1] & x[2] & x[3] & x[4])[0:24]))
# b'\x01\x15\x03'


def get_bitcode_from_master_and_index(master=masters[10], index=1):
    """return bitcode from bytesequence representing the first 24 constant values, the index of the outlet to control, and the desired state. """
    bit_sequence = numpy.unpackbits(
        numpy.array([x for x in master], dtype="uint8"), count=25
    )
    # assign index (offset 1, based on remote control)
    bit_sequence[20 - 2 * index] = 1
    bit_sequence[(20 - 2 * index) + 1] = 1
    return bit_sequence


codes_0203 = [
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
]

codes_10 = [
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
    [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0],
]
