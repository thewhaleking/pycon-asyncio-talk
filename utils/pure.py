from math import sin, cos, sqrt


def crunch(n):
    total = 0.0
    for i in range(n):
        total += sin(i * 0.001) * cos(i * 0.001) + sqrt(i * 0.5)
    return total
