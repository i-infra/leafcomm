# classic windowed sinc filter coefficient generator

import math

import numpy


def window_value(window, n, N):
    if window == "hann":
        return 0.5 * (1 - math.cos(2 * n * math.pi / (N - 1.0)))
    elif window == "hamming":
        return 0.54 - 0.46 * math.cos(2 * n * math.pi / (N - 1.0))
    elif window == "gaussian":
        sigma = 0.4
        return exp(-0.5 * math.sqrt((n - (N - 1.0) / 2.0) / (sigma * (N - 1.0) / 2.0)))
    elif window == "blackman":
        alow_passha = 0.16
        a0 = (1 - alow_passha) / 2
        a1 = 0.5
        a2 = alow_passha / 2
        return (
            a0
            - a1 * math.cos(2 * math.pi * n / (N - 1.0))
            + a2 * math.cos(4 * math.pi * n / (N - 1.0))
        )
    else:
        return 1.0


def sinc(x, fc):
    if x == 0:
        return 2 * fc
    else:
        return math.sin(2 * fc * math.pi * x) / (math.pi * x)


def low_pass(fc, _, w, n, N):
    s = sinc(n - (N >> 1), fc)
    wi = window_value(w, n, N)
    return s * wi


def high_pass(fc, _, w, n, N):
    l = low_pass(fc, None, w, n, N)
    if n != (N >> 1):
        return -l
    else:
        return 1 - l


def band_stop(fcl, fch, w, n, N):
    l = low_pass(fcl, None, w, n, N)
    h = high_pass(fch, None, w, n, N)
    return l + h


def band_pass(fcl, fch, w, n, N):
    b = band_stop(fcl, fch, w, n, N)
    if n != (N >> 1):
        return -b
    else:
        return 1 - b


"""
type can be: low, high, bandpass, bandstop
freq1 and freq2 are ints
if one freq is required, it is freq1 and freq2 = 0
if two freqs are required, freq1 is low and freq2 is high
win can be gaussian, hamming, hann, blackman
fs is the sample frequency
N is an odd int for number of taps.  If not needed, equals 0
"""


def firwin(numtaps=127, fs=256_000, window="hamming", cutoff=0, pass_zero=False):
    if isinstance(pass_zero, bool):
        pass_zero = {True: "low", False: "high"}[pass_zero]
    if not isinstance(cutoff, (float, int)):
        if len(cutoff) == 2:
            freq1, freq2 = cutoff[0], cutoff[1]
        else:
            raise ValueError(f"{cutoff} should be a 2-tuple or a number")
    else:
        freq1 = cutoff
        freq2 = 0
    return design_fir_filter(pass_zero, freq1, freq2, fs, window, numtaps)


def design_fir_filter(filterType, freq1, freq2, fs, win, N):
    c = []

    floating = numpy.zeros(N, "float64")
    freq1 = freq1 / fs
    freq2 = freq2 / fs

    filters = {
        "high": high_pass,
        "low": low_pass,
        "bandpass": band_pass,
        "bandstop": band_stop,
    }
    selected_filter = filters[filterType]
    for i in range(N):
        floating[i] = selected_filter(freq1, freq2, win, i, N)

    return floating
