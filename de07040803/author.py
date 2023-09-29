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
# events = spark.read.json("/home/student/tmp/user/master/data/events/date=2022-05-25")

# # напишите ваш код ниже
# event_day = (
#     events
#     .filter(F.col("event_type") == "reaction")
#     .groupBy(F.col("event.reaction_from"))
#     .count()
# )

# event_day.select(F.max("count")).show()
