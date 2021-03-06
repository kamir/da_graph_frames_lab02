#!/usr/bin/python

from numpy import *
import math


data_file = 'scatter1clean.csv'
histogram_file = 'histogram.dat'

# Here, angles are defined from [-180 to 180[ degrees.
# You could choose another domain. Points will be automatically
# wrapped inside that domain. This can be useful for symetrical
# side chains, for instance. Be careful to use an appropriate domain,
# otherwise the wrapping will produce meaningless data.
x_min, x_max, y_min, y_max = 0, 20.0, 0, 35000

# Number of 2D regions in which the plot is divided.
x_resolution, y_resolution = 10, 10

def read_xy(line):
    tokens = line.split(",")
    x = float(tokens[2])
    y = float(tokens[3])
    return [x, y]

points = [read_xy(line) for line in open(data_file)]

count = len(points)

histogram = zeros([x_resolution, y_resolution])

x_interval_length = (x_max - x_min) / x_resolution
y_interval_length = (y_max - y_min) / y_resolution

interval_surface = x_interval_length * y_interval_length

increment = 1000.0 / count / interval_surface

print "Increment: " ,  increment

increment = 1

for i in points:
    x = int((i[0] - x_min) / x_interval_length)
    y = int((i[1] - y_min) / y_interval_length)
    histogram[x,y] += increment
    # print x,y,histogram[x,y]

x_intervals = arange(x_min, x_max, (x_max - x_min) / x_resolution)
y_intervals = arange(y_min, y_max, (y_max - y_min) / y_resolution)

o = open(histogram_file, 'w')
for i, x in enumerate(x_intervals):
    for j, y in enumerate(y_intervals):
	if ( histogram[i,j] == 0 ):
		value = 0
	else:
		value = math.log10( histogram[i,j] )
        o.write('%f %f %f \n' % (i, j, value ))
    o.write('\n')

print histogram.max()
