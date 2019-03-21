import sys
from decimal import Decimal
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x+y)
    amounts = lines.map(lambda x: (x[2], Decimal(x[12]))).reduceByKey(lambda x, y: x+y)
    res = amounts.join(counts).map(lambda x:(x[0], x[1][0], Decimal((x[1][0]/x[1][1]).quantize(Decimal('.01')))))
    res.map(lambda x: "{0}\t{1}, {2}".format(x[0], x[1], x[2])).saveAsTextFile("task3.out")
    sc.stop()
