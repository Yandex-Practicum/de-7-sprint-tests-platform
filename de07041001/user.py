from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Сreate ODS")
    .getOrCreate()
)

events = spark.read.json("/home/student/user/master/data/events")

# напишите ваш код ниже
