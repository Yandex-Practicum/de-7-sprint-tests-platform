from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

# напишите ваш код ниже
events = spark.read.json("/home/student/user/master/data/events/date=2022-05-25")
events.show(10)
