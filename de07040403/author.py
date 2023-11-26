# # ВАЖНО: для проверки авторское выполнять не нужно
# # Раскомментируйте если будете передавать решение студенту

# from pyspark.sql import SparkSession

# spark = (
#     SparkSession
#     .builder
#     .master("yarn")
#     .appName("Learning DataFrames")
#     .getOrCreate()
# )

# df = spark.read.parquet("/home/student/tmp/user/master/data/snapshots/channels/actual")

# # напишите ваш код ниже
# (
#     df.write
#     .partitionBy("channel_type")
#     .mode("append")
#     .parquet("/home/student/tmp/user/USERNAME/analytics/test")
# )

# df_final = spark.read.parquet("/home/student/tmp/user/USERNAME/analytics/test")
# df_final.select("channel_type").orderBy("channel_type").distinct().show()
