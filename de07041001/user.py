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
