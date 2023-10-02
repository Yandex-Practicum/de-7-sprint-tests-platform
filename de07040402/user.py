from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("yarn")
    .appName("Learning DataFrames")
    .getOrCreate()
)

# напишите ваш код ниже
