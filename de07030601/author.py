# напишите ваш код ниже
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("My first session")
    .getOrCreate()
)