# # ВАЖНО: для проверки авторское выполнять не нужно
# # Раскомментируйте если будете передавать решение студенту

# import pyspark.sql.functions as F
# from pyspark.sql import SparkSession

# spark = (
#     SparkSession
#     .builder
#     .master("yarn")
#     .appName("Learning DataFrames")
#     .getOrCreate()
# )
# events = spark.read.json("/home/student/tmp/user/master/data/events/date=2022-05-31")

# # напишите ваш код ниже
# print(events.filter(F.col("event.message_to").isNotNull()).count())
