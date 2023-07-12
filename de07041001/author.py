# ВАЖНО: для экономии ресурсов авторское выполнять не нужно
# Раскомментируйте, если будете передавать решение студенту

# import pyspark.sql.functions as F
# from pyspark.sql import SparkSession

# PATH = "/home/student/tmp/user/USERNAME/data/events"

# spark = (
#     SparkSession.builder
#     .master("local")
#     .appName("Сreate ODS")
#     .getOrCreate()
# )

# events = spark.read.json("/home/student/user/master/data/events")

# # напишите ваш код ниже
# (
#     events.write
#     .partitionBy("date", "event_type")
#     .mode("overwrite")
#     .parquet(PATH)
# )

# (
#     spark.read
#     .parquet(PATH)
#     .orderBy(F.col("event.datetime").desc())
#     .show(10)
# )
