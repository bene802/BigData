#!/usr/bin/env python

import sys
import string

currentkey = None
dic = {}
dic2 = {}

for line in sys.stdin:
    try:
        line = line.strip()
        key, value = line.split('\t')
        summon, date = value.split(',')
        
        if key == currentkey:
            if date == '2016-03-05' or date == '2016-03-06' or date == '2016-03-12' or date == '2016-03-13' or date == '2016-03-19' or date == '2016-03-20' or date == '2016-03-26' or date == '2016-03-27':
                dic[key] += 1
            else:
                dic2[key] += 1
        else:
            if currentkey:
                ave = float(dic.get(currentkey)/8.0)
                ave2 = float(dic2.get(currentkey)/23.0)
                print('{0:s}\t{1:.2f}, {2:.2f}'.format(currentkey, ave, ave2))
            currentkey = key
            if date == '2016-03-05' or date == '2016-03-06' or date == '2016-03-12' or date == '2016-03-13' or date == '2016-03-19' or date == '2016-03-20' or date == '2016-03-26' or date == '2016-03-27':
                dic[key] = 1
                dic2[key] = 0
            else:    
                dic2[key] = 1
                dic[key] = 0
    except:
        continue
ave = float(dic.get(currentkey)/8.0)
ave2 = float(dic2.get(currentkey)/23.0)
print('{0:s}\t{1:.2f}, {2:.2f}'.format(key, ave, ave2))


 
