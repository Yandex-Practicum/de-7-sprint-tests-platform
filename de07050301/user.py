from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").getOrCreate()


# напишите ваш код ниже
...

result = ...