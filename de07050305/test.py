# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 5
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#ce9d1dff0b614b6194297746ee4ff4d8

def _user_precode():
    global __name__
    __name__ = "__main__"
    import sys

    sys.argv = [
        'user_interests.py',
        '2022-05-04',
        '3',
        '/home/student/user/USERNAME/data/events',
        '/home/student/tmp/user/USERNAME/data/analytics/user_interests',
    ]

# генерируем данные для проверки
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

_spark = (
    SparkSession.builder
    .getOrCreate()
)

_date = "2022-05-04"
_days_count = "3"
_events_base_path = "/home/student/user/USERNAME/data/events"

def _input_event_paths(_base_path, _date, _depth):
    _dt = datetime.datetime.strptime(_date, "%Y-%m-%d")
    return [
        f"{_base_path}/date={(_dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(int(_depth))
    ]


def _calculate_user_interests(_posts_part, _posts_all, _reactions):
    _post_tops = tag_tops(_posts_part)
    _reaction_tops = _reaction_tag_tops(_posts_all, _reactions)
    return _post_tops.join(_reaction_tops, "user_id", "full_outer")


def _tag_tops(_posts):
    _result = (
        _posts
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

    return _result


def _reaction_tag_tops(_posts_all, _reactions):
    _all_message_tags = _posts_all.select(
        F.col("event.message_id").alias("message_id"),
        F.col("event.message_from").alias("user_id"),
        F.explode(F.col("event.tags")).alias("tag"),
    )

    _reaction_tags = _reactions.select(
        F.col("event.reaction_from").alias("user_id"),
        F.col("event.message_id").alias("message_id"),
        F.col("event.reaction_type").alias("reaction_type"),
    ).join(_all_message_tags.select("message_id", "tag"), "message_id")

    _reaction_tops = (
        _reaction_tags.groupBy("user_id", "tag", "reaction_type")
        .agg(F.count("*").alias("tag_count"))
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id", "reaction_type").orderBy(F.desc("tag_count"), F.desc("tag"))))
        .where("rank <= 3")
        .groupBy("user_id", "reaction_type")
        .pivot("rank", [1, 2, 3])
        .agg(F.first("tag"))
        # .cache()
    )

    _like_tops = (
        _reaction_tops.where("reaction_type = 'like'")
        .drop("reaction_type")
        .withColumnRenamed("1", "like_tag_top_1")
        .withColumnRenamed("2", "like_tag_top_2")
        .withColumnRenamed("3", "like_tag_top_3")
    )

    _dislike_tops = (
        _reaction_tops.where("reaction_type = 'dislike'")
        .drop("reaction_type")
        .withColumnRenamed("1", "dislike_tag_top_1")
        .withColumnRenamed("2", "dislike_tag_top_2")
        .withColumnRenamed("3", "dislike_tag_top_3")
    )

    _result = _like_tops.join(_dislike_tops, "user_id", "full_outer")

    return _result

_posts_part = (
    _spark.read
    .option("basePath", _events_base_path)
    .parquet(*_input_event_paths(_events_base_path, _date, _days_count))
    .where("event.message_channel_to is not null and event_type = 'message'")
)

_posts_all = (
    _spark.read
    .parquet(_events_base_path)
    .where("event_type='message' and event.message_channel_to is not null")
)

_reactions_part = (
    _spark.read
    .option("basePath", _events_base_path)
    .parquet(*_input_event_paths(_events_base_path, _date, _days_count))
    .where("event_type = 'reaction'")
)

_a_interests = _calculate_user_interests(_posts_part, _posts_all, _reactions_part)
# сгенерировали данные для проверки

_u_interests = _spark.read.parquet(
    f"/home/student/tmp/user/USERNAME/data/analytics/user_interests/date={_date}"
)

_test.cmp(
    _u_interests.toPandas(),
    _a_interests.toPandas(),
    m="Проверьте как формируете данные на запись.",
)