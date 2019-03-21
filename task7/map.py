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
    violation_code = entry[2]
    summons_number = entry[0]
    date = entry[1]
    print('{0:s}\t{1:s},{2:s}'.format(violation_code, summons_number, date))

