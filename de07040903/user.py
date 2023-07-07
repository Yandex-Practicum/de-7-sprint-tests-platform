from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)
events = spark.read.json("/home/student/user/master/data/events/date=2022-05-01")
# напишите ваш код ниже
