import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)
events = spark.read.json("/home/student/user/master/data/events/date=2022-05-01")
# напишите ваш код ниже
window = Window().partitionBy("event.message_from").orderBy("event.message_from")
dfWithLag = events.withColumn("lag_7", F.lag("event.message_to", 7).over(window))
(
    dfWithLag
    .select("event.message_from", "lag_7")
    .filter(dfWithLag.lag_7.isNotNull())
    .orderBy(F.col("event.message_to").desc())
    .show(10, False)
)
