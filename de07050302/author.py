import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local")
    # .config("spark.sql.shuffle.partitions", "1")
    # .config('spark.executor.instances', '1')
    # .config('spark.dynamicAllocation.enabled', 'false')
    .getOrCreate()
)


# напишите ваш код ниже
def input_event_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/home/student/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(depth)
    ]


def reaction_tag_tops(date, depth, spark):
    reaction_paths = input_event_paths(date, depth)
    reactions = (
        spark.read
        .option("basePath", "/home/student/user/USERNAME/data/events")
        .parquet(*reaction_paths)
        .where("event_type='reaction'")
    )

    all_message_tags = (
        spark.read
        .parquet("/home/student/user/USERNAME/data/events")
        .where("event_type='message' and event.message_channel_to is not null")
        .select(
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_from").alias("user_id"),
            F.explode(F.col("event.tags")).alias("tag"),
        )
    )

    reaction_tags = reactions.select(
        F.col("event.reaction_from").alias("user_id"),
        F.col("event.message_id").alias("message_id"),
        F.col("event.reaction_type").alias("reaction_type"),
    ).join(all_message_tags.select("message_id", "tag"), "message_id")

    reaction_tops = (
        reaction_tags.groupBy("user_id", "tag", "reaction_type")
        .agg(F.count("*").alias("tag_count"))
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("tag_count"), F.desc("tag"))))
        .where("rank <= 3")
        .groupBy("user_id", "reaction_type")
        .pivot("rank", [1, 2, 3])
        .agg(F.first("tag"))
    )

    like_tops = (
        reaction_tops
        .where("reaction_type = 'like'")
        .drop("reaction_type")
        .withColumnRenamed("1", "like_tag_top_1")
        .withColumnRenamed("2", "like_tag_top_2")
        .withColumnRenamed("3", "like_tag_top_3")
    )

    dislike_tops = (
        reaction_tops
        .where("reaction_type = 'dislike'")
        .drop("reaction_type")
        .withColumnRenamed("1", "dislike_tag_top_1")
        .withColumnRenamed("2", "dislike_tag_top_2")
        .withColumnRenamed("3", "dislike_tag_top_3")
    )

    result = like_tops.join(dislike_tops, "user_id", "full_outer")

    return result


result = reaction_tag_tops("2022-05-04", 3, spark)