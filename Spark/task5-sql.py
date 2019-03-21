import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import string

spark = SparkSession \
    .builder \
    .appName("task5-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
res = spark.sql("SELECT plate_id, registration_state, COUNT(*) \
           FROM parking \
           GROUP BY plate_id, registration_state \
           ORDER BY COUNT(*) DESC LIMIT 1")
res.select(format_string('%s, %s\t%d',res.plate_id,res.registration_state,res['count(1)'])).write.save("task5-sql.out",format="text")





