from __future__ import print_function
import sys
from csv import reader
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    lines1 = sc.textFile(sys.argv[1], 1)
    lines1 = lines1.mapPartitions(lambda x : reader(x))
    lines2 = sc.textFile(sys.argv[2], 1)
    lines2 = lines2.mapPartitions(lambda x : reader(x))
    res1 = lines1.map(lambda x: (x[0],(x[14],x[6],x[2],x[1])))
    res2 = lines2.map(lambda x: (x[0],1))
    res = res1.subtractByKey(res2)
    res = res.sortByKey().map(lambda x: (x[0],x[1][0],x[1][1],x[1][2], x[1][3]))
    res.map(lambda x: "{0}\t{1}, {2}, {3}, {4}".format(x[0], x[1], x[2], x[3], x[4])).saveAsTextFile("task1.out")
    sc.stop()





