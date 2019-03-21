import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    lines = lines.map(lambda x: ((x[14], x[16]), 1)).reduceByKey(lambda x, y: x+y)
    line = lines.sortBy(lambda x: x[1], False).take(1)
    res = sc.parallelize(line).map(lambda x: (x[0][0], x[0][1], x[1]))
    res.map(lambda x: "{0}, {1}\t{2}".format(x[0], x[1], x[2])).saveAsTextFile("task5.out")
    sc.stop()    

