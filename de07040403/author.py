from pyspark.sql import SparkSession

# ВАЖНО: это путь в каталог AUTHOR
# Замените AUTHOR на USERNAME, если будете передавать решение студенту
PATH = "/home/student/tmp/user/AUTHOR/analytics/test"

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)

df = spark.read.parquet("/home/student/user/master/data/snapshots/channels/actual")

# напишите ваш код ниже
(
    df.write
    .partitionBy("channel_type")
    .mode("append")
    .parquet(PATH)
)

df_final = spark.read.parquet(PATH)
df_final.select("channel_type").orderBy("channel_type").distinct().show()
