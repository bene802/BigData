import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import date_format
import string

spark = SparkSession \
    .builder \
    .appName("task2-sql") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
opening = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
opening.createOrReplaceTempView("opening")
parking.createOrReplaceTempView("parking")
result = spark.sql("select P.summons_number, P.plate_id, P.violation_precinct, P.violation_code, P.issue_date from parking as P left join opening as O on P.summons_number = O.summons_number where O.summons_number is null")


result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")









