import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession \
    .builder \
    .appName("task3-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
res = spark.sql("SELECT license_type, SUM(amount_due), AVG(amount_due) FROM parking GROUP BY license_type")
res.select(format_string('%s\t%.2f, %.2f',res.license_type,res['SUM(amount_due)'],res['AVG(amount_due)'])).write.save("task3-sql.out",format="text")


