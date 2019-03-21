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
    state = entry[16]
    summons_number = entry[0]
    if state == 'NY':
        key = 'NY'
    else:        
        key = 'Other'
    print('{0:s}\t{1:s}'.format(key, summons_number))


