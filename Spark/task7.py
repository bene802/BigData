import sys
from csv import reader
from pyspark import SparkContext
from decimal import Decimal

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    res1 = lines.map(lambda x: (x[2],(0 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 1)))
    res2 = res1.reduceByKey(lambda x, y: x + y)
    res3 = lines.map(lambda x: (x[2],(1 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 0)))  
    res4 = res3.reduceByKey(lambda x, y: x + y)
    
    res5 = res2.map(lambda x: (x[0], Decimal(Decimal(x[1])/23).quantize(Decimal('.01'))))
    res6 = res4.map(lambda x: (x[0], Decimal(Decimal(x[1])/8).quantize(Decimal('.01'))))
    res6.fullOuterJoin(res5).map(lambda x: (x[0],x[1][0],x[1][1])).map(lambda x: "{0}\t{1}, {2}".format(x[0], x[1], x[2])).saveAsTextFile("task7.out")
    sc.stop()

