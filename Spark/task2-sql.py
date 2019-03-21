import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import string

spark = SparkSession \
    .builder \
    .appName("task2-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
res = parking.groupBy("violation_code").count()
res.select(format_string('%d\t%d',res.violation_code,res['count'])).write.save("task2-sql.out",format="text")

