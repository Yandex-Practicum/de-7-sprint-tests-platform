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

# # напишите ваш код ниже
# events = spark.read.json("/home/student/tmp/user/master/data/events/date=2022-05-25")
# events.show(10)
