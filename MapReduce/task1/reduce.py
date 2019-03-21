#!/usr/bin/env python
import sys
import string

currentkey = None
currentvalue = None
count = 0
flag = False

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t',1)
    if key == currentkey:
        if value != 'open':
            flag = True
        count += 1
    else:
        if currentkey:
            if count == 1 and flag == True:
                print('{0:s}\t{1:s}'.format(currentkey, currentvalue))
        if value == 'open':
            flag = False
        else:
            flag = True
        currentkey = key
        currentvalue = value
        count = 1
if count == 1 and flag == True:
    print('{0:s}\t{1:s}'.format(key, value))
