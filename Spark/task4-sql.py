import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
import string

spark = SparkSession \
    .builder \
    .appName("task4-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")

res = spark.sql("SELECT \
           CASE WHEN registration_state LIKE '%NY%' THEN 'NY' \
           ELSE 'Other' END AS state \
    FROM parking")

res1 = res.groupBy("state").count()
res1.select(format_string('%s\t%d',res1.state,res1['count'])).write.save("task4-sql.out",format="text")



