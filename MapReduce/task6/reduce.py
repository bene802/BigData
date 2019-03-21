#!/usr/bin/env python

import sys
import string
import operator
record = {}
currentkey = None
count = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    if key == currentkey:
        count += 1
    else:
        if currentkey:
            record[currentkey] = count
        currentkey = key
        count = 1

record[key] = count
sort = sorted(record.items(), key=operator.itemgetter(1), reverse=True)
res = sort[:20]
for k,v in res:
    print('{0:s}\t{1:d}'.format(k, v))





    
        
