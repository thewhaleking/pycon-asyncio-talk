# go_fast.pyx
from libc.math cimport sin, cos, sqrt
from libc.stdint cimport int64_t
cimport cython


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
def crunch(int64_t n):
    """CPU-intensive computation that releases the GIL."""
    cdef int64_t i
    cdef double total = 0.0

    with nogil:
        for i in range(n):
            total += sin(i * 0.001) * cos(i * 0.001) + sqrt(i * 0.5)

    return total
