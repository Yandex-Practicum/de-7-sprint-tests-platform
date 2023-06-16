from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

# напишите ваш код ниже
