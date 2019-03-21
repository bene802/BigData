import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x))
    lines_NY = lines.filter(lambda x: x[16] == 'NY').map(lambda x: (x[16], 1)).reduceByKey(lambda x, y: x+y)
    lines_NOTNY = lines.filter(lambda x: x[16] != 'NY').map(lambda x: ('Other', 1)).reduceByKey(lambda x, y: x+y)
    res = lines_NY.union(lines_NOTNY)
    res.map(lambda x: "{0}\t{1}".format(x[0], x[1])).saveAsTextFile("task4.out")
    sc.stop()    

