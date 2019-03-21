#!/usr/bin/env python

import sys
import string

amount = 0.00
count = 1
currentkey = None

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    value = float(value)
    if key == currentkey:
        count = count + 1
        amount = amount + value
    else:
        if currentkey:
            aver = amount/count
            print('{0:s}\t{1:.2f}, {2:.2f}'.format(currentkey, amount, aver))
        currentkey = key
        amount = value
        count = 1
 

aver = amount/count
print('{0:s}\t{1:.2f}, {2:.2f}'.format(key, amount, aver))
    

