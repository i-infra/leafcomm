import numpy as np

import pyfftw


def stftw(input_vector, sampling_rate, frame_duration, increment_duration):
    frame_length = int(frame_duration * sampling_rate)
    increment_amount = int(increment_duration * sampling_rate)
    starting_points = range(0, len(input_vector) - frame_length, increment_amount)
    input_array = pyfftw.empty_aligned(frame_length, dtype="complex64")
    output_array = pyfftw.empty_aligned(frame_length, dtype="complex64")
    fft_runner = pyfftw.FFTW(input_array, output_array)
    window = np.hanning(frame_length).astype("complex64")
    X = np.zeros((len(starting_points) + 1, frame_length), dtype="complex64")
    acc = 0
    for i in starting_points:
        np.copyto(
            input_array, window * input_vector[i : i + frame_length], casting="no"
        )
        fft_runner()
        np.copyto(X[acc], np.array(output_array))
        acc += 1
    return X.reshape((acc + 1, frame_length))


def stft(x, fs, framesz, hop):
    framesamp = int(framesz * fs)
    hopsamp = int(hop * fs)
    w = np.hanning(framesamp)
    X = np.array(
        [
            np.fft.fft(w * x[i : i + framesamp])
            for i in range(0, len(x) - framesamp, hopsamp)
        ]
    )
    return X


def istft(X, fs, T, hop):
    x = np.zeros(T * fs)
    framesamp = X.shape[1]
    hopsamp = int(hop * fs)
    for n, i in enumerate(range(0, len(x) - framesamp, hopsamp)):
        x[i : i + framesamp] += np.real(np.fft.ifft(X[n]))
    return x
