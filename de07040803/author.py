import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)
events = spark.read.json("/home/student/user/master/data/events/date=2022-05-25")

# напишите ваш код ниже
event_day = (
    events
    .filter(F.col("event_type") == "reaction")
    .groupBy(F.col("event.reaction_from"))
    .count()
)

event_day.select(F.max("count")).show()
