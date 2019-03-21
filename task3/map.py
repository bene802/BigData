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
    license_type = entry[2]
    amount_due = entry[12]
    print('{0:s}\t{1:s}'.format(license_type, amount_due))
