from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

# напишите ваш код ниже
