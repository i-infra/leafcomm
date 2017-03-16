import timeit
import bottleneck as bn
import numpy as np
import numba as nu
import numexpr3 as ne3
import numexpr as ne

def nuabs(x):
    y = np.zeros(x.shape, dtype='float32')
    shape = x.shape[0]
    @nu.jit('float32[:](complex64[:], float32[:])', nopython=True,nogil=True,cache=True) 
    def nuabs_(x,y):
        for i in range(x.shape[0]):
            y[i] = np.abs(x[i])
        return y
    nuabs_(x,y)
    return y

@nu.jit('float32(float32[:])',nopython=True,nogil=True,cache=True)
def numax(x):
   return np.max(x)

def ne3abs(x):
    y = np.zeros(x.shape, dtype='float32')
    ne3.evaluate('y=abs(x)')
    return y

def nesum(x):
    y = ne.evaluate('sum(x)')
    return y

def nemax(x):
    y = ne.evaluate('max(x,axis=0)')
    return y

x = np.random.random(2**20).astype('float32')
xj = np.random.random(2**20).astype('float32') + 1j*x

# warm up numba cache
numax(x)
nuabs(xj)

timer = lambda f: (f,timeit.timeit(f+'(x)', number=100, globals=globals())/100)
timerj = lambda f: (f,timeit.timeit(f+'(xj)', number=10, globals=globals())/10)

print(timer('(lambda x:x[np.argmax(x)])'))
print(timer('np.max'))
print(timer('bn.nanmax'))
print(timer('numax'))
print(timer('nemax'))
print(timerj('np.abs'))
print(timerj('nuabs'))
print(timerj('ne3abs'))
print(timer('np.sum'))
print(timer('bn.nansum'))
print(timer('nesum'))
