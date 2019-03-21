import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import col, avg
from pyspark.sql.functions import lit
import string

spark = SparkSession \
    .builder \
    .appName("task2-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")

a = parking.select("violation_code").distinct()
b = parking.where((col("issue_date") == "2016-03-05") | (col("issue_date") == "2016-03-06") | (col("issue_date") == "2016-03-13") | (col("issue_date") == "2016-03-12") | (col("issue_date") == "2016-03-19") | (col("issue_date") == "2016-03-20") | (col("issue_date") == "2016-03-26") | (col("issue_date") == "2016-03-27")).select("violation_code").distinct()
a.createOrReplaceTempView("a")
b.createOrReplaceTempView("b")
zero1 = spark.sql("SELECT a.violation_code FROM a \
          LEFT OUTER JOIN b \
          ON (a.violation_code = b.violation_code) \
          WHERE b.violation_code IS NULL")

zero11 = zero1.withColumn('count', lit(0))

res1 = parking.filter((col("issue_date") == "2016-03-05") | (col("issue_date") == "2016-03-06") | (col("issue_date") == "2016-03-13") | (col("issue_date") == "2016-03-12") | (col("issue_date") == "2016-03-19") | (col("issue_date") == "2016-03-20") | (col("issue_date") == "2016-03-26") | (col("issue_date") == "2016-03-27")) \
.groupBy("violation_code").count()

new_res = res1.union(zero11)

res2 = new_res.withColumn('avg_ends', new_res["count"]/8.0)

c = parking.where((col("issue_date") != "2016-03-05") & (col("issue_date") != "2016-03-06") & (col("issue_date") != "2016-03-13") & (col("issue_date") != "2016-03-12") & (col("issue_date") != "2016-03-19") & (col("issue_date") != "2016-03-20") & (col("issue_date") != "2016-03-26") & (col("issue_date") != "2016-03-27")).select("violation_code").distinct()

c.createOrReplaceTempView("c")
zero2 = spark.sql("SELECT a.violation_code FROM a \
          LEFT OUTER JOIN c \
          ON (a.violation_code = c.violation_code) \
          WHERE c.violation_code IS NULL")
zero22 = zero2.withColumn('count', lit(0))

res3 = parking.filter((col("issue_date") != "2016-03-05") & (col("issue_date") != "2016-03-06") & (col("issue_date") != "2016-03-13") & (col("issue_date") != "2016-03-12") & (col("issue_date") != "2016-03-19") & (col("issue_date") != "2016-03-20") & (col("issue_date") != "2016-03-26") & (col("issue_date") != "2016-03-27")) \
.groupBy("violation_code").count()

new_res2 = res3.union(zero22)

res4 = new_res2.withColumn('avg_days', new_res2["count"]/23.0)

res = res4.join(res2, ["violation_code"])

res.select(format_string('%d\t%.2f, %.2f',res.violation_code,res.avg_ends,res.avg_days)).write.save("task7-sql.out",format="text")




