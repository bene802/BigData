import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    counts = lines.map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x+y)
    counts.map(lambda x: "{0}\t{1}".format(x[0], x[1])).saveAsTextFile("task2.out")
    sc.stop()
