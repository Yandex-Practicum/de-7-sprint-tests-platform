import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("yarn")
    .appName("Сreate ODS")
    .getOrCreate()
)

events = spark.read.json("/home/student/tmp/user/master/data/events")

# напишите ваш код ниже
(
    events.write
    .partitionBy("date", "event_type")
    .mode("overwrite")
    .parquet("/home/student/tmp/user/USERNAME/data/events")
)

(
    spark.read
    .parquet("/home/student/tmp/user/USERNAME/data/events")
    .orderBy(F.col("event.datetime").desc())
    .show(10)
)
