#!/usr/bin/env python
import sys
import string
import os
import csv
from io import StringIO

filename = os.environ.get('mapreduce_map_input_file')

for line in sys.stdin:
    line = line.strip()
    data = StringIO(line)
    read = csv.reader(data, delimiter=',')
    entry = list(read)[0]
    summons_number = entry[0]
    if 'parking' in filename:
        plate_id = entry[14]
        violation_precinct = entry[6]
        violation_code = entry[2]
        issue_date = entry[1]
        print('{0:s}\t{1:s}, {2:s}, {3:s}, {4:s}'.format(summons_number, plate_id, violation_precinct, violation_code, issue_date))    
    if 'open' in filename:
        print('{0:s}\topen'.format(summons_number))

