#!/usr/bin/env python

import sys
import string
count = 0
currentkey = None
temp_value = ''
for line in sys.stdin:
    try:
        line = line.strip()
        key, value = line.split('\t')
        key = str(key)
        value = str(value)
        if key == currentkey:
            count += 1
            temp_value = value
        else:
            if currentkey:
                res = currentkey[1:]
                print('{0:s}\t{1:s}, {2:d}'.format(temp_value, res, count))
            currentkey = key
            temp_value = value
            count = 1
    except:
        continue
res = key[1:]
print('{0:s}\t{1:s}, {2:d}'.format(value, res, count))



