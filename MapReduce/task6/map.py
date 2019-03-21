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
    plate_id = entry[14]
    registration_state = entry[16]
    print('{0:s}, {1:s}\t1'.format(plate_id, registration_state))
   
