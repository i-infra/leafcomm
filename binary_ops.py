import itertools
import typing


def rle(xs):
    return [(len(list(gp)), x) for x, gp in itertools.groupby(xs)]


def rld(xs):
    return itertools.chain.from_iterable((itertools.repeat(x, n) for n, x in xs))


def debinary(ba):
    return sum([x * 2 ** i for i, x in enumerate(reversed(ba))])


def flatten(l):
    return list(itertools.chain(*[[x] if type(x) not in [list] else x for x in l]))


def rerle(xs):
    return [
        (sum([i[0] for i in x[1]]), x[0]) for x in itertools.groupby(xs, lambda x: x[1])
    ]


def extract_fields_from_bits(
    bits: typing.List[int], bitwidths: typing.List[int], output_class=list
):
    """ accept a bit vector, list of field bitwidths, and output datatype. do the segmenting and binary conversion. """
    if bitwidths[0] != 0:
        bitwidths = [0] + bitwidths
    field_positions = [x for x in itertools.accumulate(bitwidths)]
    results = [
        debinary(bits[x:y]) for x, y in zip(field_positions, field_positions[1:])
    ]
    return output_class(*results)
