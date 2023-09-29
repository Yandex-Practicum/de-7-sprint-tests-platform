import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.master("yarn").getOrCreate()


# напишите ваш код ниже
def input_event_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/home/student/tmp/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(depth)
    ]


def tag_tops(date, depth, spark):
    message_paths = input_event_paths(date, depth)
    result = (
        spark.read
        .option("basePath", "/home/student/tmp/user/USERNAME/data/events")
        .parquet(*message_paths)
        .where("event_type = 'message'")
        .where("event.message_channel_to is not null")
        .select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.explode(F.col("event.tags")).alias("tag"),
        )
        .groupBy("user_id", "tag")
        .agg(F.count("*").alias("tag_count"))
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("tag_count"), F.desc("tag"))))
        .where("rank <= 3")
        .groupBy("user_id")
        .pivot("rank", [1, 2, 3])
        .agg(F.first("tag"))
        .withColumnRenamed("1", "tag_top_1")
        .withColumnRenamed("2", "tag_top_2")
        .withColumnRenamed("3", "tag_top_3")
    )

    return result


result = tag_tops("2022-05-25", 7, spark)
