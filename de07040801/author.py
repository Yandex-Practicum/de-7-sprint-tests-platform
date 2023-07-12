import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)
events = spark.read.json("/home/student/user/master/data/events/date=2022-05-31")

# напишите ваш код ниже
(
    events
    .withColumn("hour", F.hour(F.col("event.datetime")))
    .withColumn("minute", F.minute(F.col("event.datetime")))
    .withColumn("second", F.second(F.col("event.datetime")))
    .orderBy(F.col("event.datetime").desc())
    .show(10)
)
