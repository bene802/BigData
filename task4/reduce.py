#!/usr/bin/env python

import sys
import string

currentkey = None
count = 0

for line in sys.stdin:
    line.strip()
    key, value = line.split('\t')
    if key == currentkey:
        count += 1
    else:
        if currentkey:
            print('{0:s}\t{1:d}'.format(currentkey, count))
        currentkey = key
        count = 1     
print('{0:s}\t{1:d}'.format(currentkey, count))        
