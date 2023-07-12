# напишите ваш код ниже
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("My first session")
    .getOrCreate()
)

a = 1 # Lol kek cheburek
# b = {
#     'a': 1,
    #     'b': 2,
#     'c': 3
#     }