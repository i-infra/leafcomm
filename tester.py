import beepshrink, packetizer, cbor
import numpy as np
import sys

import time

# accepts a '.beep' filename as the argument, attempts to run through early stage packetization / framing

weighted_center = lambda X: int(np.average(range(X.shape[1]), weights=np.sum(np.absolute(X), axis=0)))

f = open(sys.argv[1], 'rb').read()
g = cbor.loads(f)
h = beepshrink.decompress(**g)
a = np.absolute(h)


from scipy.signal import butter, lfilter, freqz

def butter_lowpass(cutoff, fs, order=5):
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    return b, a

def butter_lowpass_filter(data, cutoff, fs, order=5):
    b, a = butter_lowpass(cutoff, fs, order=order)
    y = lfilter(b, a, data)
    return y
import bottleneck as bn
start = time.time()
#a = butter_lowpass_filter(a, 2e3, 256e3, 5)
ap = bn.move_mean(a, 16, 1)
#ap = np.convolve(a, [1/128]*128)
end = time.time()

print(end-start)

print(np.max(a), np.mean(a))
pulses = [x for x in packetizer.rle(ap > np.max(ap)/2)]
packets = packetizer.demodulator(pulses)
for packet in packets:
    print(packetizer.printer(packet.packet))
    print(packet.errors, packet.deciles)
    if packet.errors == []:
        break

if __name__ == "__console__":
    import stft
    start = time.time()
    #h += np.cos(-32000*np.linspace(0,len(h)/256000, len(h))*2*np.pi)/100.
    #h -= 1j*np.sin(-32000*np.linspace(0,len(h)/256000, len(h))*2*np.pi)/100.
    H = stft.stftw(h, 256000, 0.0005, 0.0005)
    end = time.time()
    print(end-start)
    centroid = weighted_center(H)
    width = H.shape[1]
    M = np.array([[1]*(centroid)+[0]*(width-centroid)]*H.shape[0])
    import scipy.signal
    Hprime = np.sum(np.absolute(H), axis=0)
    Hprime = Hprime*(Hprime>np.mean(Hprime[Hprime>np.mean(Hprime)]))
    centroids = scipy.signal.argrelextrema(Hprime, np.greater)
    freqs = np.fft.fftfreq(width, 1./256000)
    import matplotlib
    matplotlib.use('qt5agg')
    from matplotlib import pyplot
    pyplot.figure()
    pyplot.title("sum(abs(H*M)) - time")
    #pyplot.plot(a)
    #pyplot.plot(ap)
    pyplot.plot(np.sum(np.absolute(H*M), axis=1))
    pyplot.figure()
    pyplot.title("sum(abs(H)) - freq")
    pyplot.plot(np.sum(np.absolute(H), axis=0))
    pyplot.axvline(centroid, color='r', linewidth=4)
    for centroid in centroids[0]:
        print(freqs[int(centroid)])
        pyplot.axvline(centroid, color='r', linewidth=4)
    pyplot.figure()
    pyplot.title("abs(H) - waterfall")
    pyplot.imshow(np.absolute(H), origin='lower', aspect='auto',interpolation='nearest')
    pyplot.figure()
    pyplot.title("abs(H*M) - waterfall")
    pyplot.imshow(np.absolute(H*M), origin='lower', aspect='auto',interpolation='nearest')
    pyplot.figure()
    pyplot.title("abs(H*!M) - waterfall")
    pyplot.imshow(np.absolute(H*(M==0)), origin='lower', aspect='auto',interpolation='nearest')
    pyplot.show()
