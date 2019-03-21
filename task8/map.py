#!/usr/bin/env python

import sys
import string
import csv
from io import StringIO

for line in sys.stdin: 
    line = line.strip()
    data = StringIO(line)
    read = csv.reader(data, delimiter=',')
    entry = list(read)[0]
    color = entry[19]
    make = entry[20]
    if make == '':
        print('1NONE\tvehicle_make')
    else:
        make = '1'+make
        print('{0:s}\tvehicle_make'.format(make))
    if color == '':
        print('2NONE\tvehicle_color')
    else:
        color = '2'+color
        print('{0:s}\tvehicle_color'.format(color))
 
    
