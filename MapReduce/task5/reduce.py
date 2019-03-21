#!/usr/bin/env python

import sys
import string

currentkey = None
key_max = ''
count_max = 0
count = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    if key == currentkey:
        count += 1
    else:
        if currentkey:
            if count > count_max:
                key_max = currentkey
                count_max = count
        currentkey = key
        count = 1
  
if count > count_max:
    key_max = key
    count_max = count
print('{0:s}\t{1:d}'.format(key_max, count_max))

        
