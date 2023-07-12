# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 3: портрет контактов пользователя / Задание 1
# [https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc2f2b3-045c-4c23-b6bb-a1c85d302074/theory/](https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc2f2b3-045c-4c23-b6bb-a1c85d302074/theory/)
# [https://www.notion.so/praktikum/4-3-cbbe0f4e30d74f0293bda436599a4ab6?pvs=4#d6d70723db2f45d8ae61d43c53eb5e77](https://www.notion.so/praktikum/4-3-cbbe0f4e30d74f0293bda436599a4ab6?pvs=4#d6d70723db2f45d8ae61d43c53eb5e77)

def _user_precode():
    global __name__
    __name__ = "__main__"
    import sys

    sys.argv = [
        'connection_interests.py',
        '2022-05-04',
        '3',
        '/home/student/user/USERNAME/data/events',
        '/home/student/tmp/user/USERNAME/data/analytics/user_interests',
        '/home/student/user/master/data/snapshots/tags_verified/actual',
        '/home/student/tmp/user/USERNAME/data/analytics/connection_interests',
    ]

    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window
    import datetime

    _spark = SparkSession.builder.master('local').getOrCreate()

    _date = '2022-05-04'
    _days_count = '3'
    _events_base_path = '/home/student/user/USERNAME/data/events'
    _output_base_path = '/home/student/tmp/user/USERNAME/data/analytics/user_interests'

    def _input_event_paths(_base_path, _date, _depth):
        _dt = datetime.datetime.strptime(_date, '%Y-%m-%d')
        return [
            f"{_base_path}/date={(_dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
            for x in range(int(_depth))
        ]


    def _calculate_user_interests(_posts_part, _posts_all, _reactions):
        _post_tops = _tag_tops(_posts_part)
        _reaction_tops = _reaction_tag_tops(_posts_all, _reactions)
        return _post_tops.join(_reaction_tops, 'user_id', 'full_outer')


    def _tag_tops(_posts):
        _result = (
            _posts.select(
                F.col('event.message_id').alias('message_id'),
                F.col('event.message_from').alias('user_id'),
                F.explode(F.col('event.tags')).alias('tag'),
            )
            .groupBy('user_id', 'tag')
            .agg(F.count('*').alias('tag_count'))
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.partitionBy('user_id').orderBy(
                        F.desc('tag_count'), F.desc('tag')
                    )
                ),
            )
            .where('rank <= 3')
            .groupBy('user_id')
            .pivot('rank', [1, 2, 3])
            .agg(F.first('tag'))
            .withColumnRenamed('1', 'tag_top_1')
            .withColumnRenamed('2', 'tag_top_2')
            .withColumnRenamed('3', 'tag_top_3')
        )

        return _result


    def _reaction_tag_tops(_posts_all, _reactions):
        _all_message_tags = _posts_all.select(
            F.col('event.message_id').alias('message_id'),
            F.col('event.message_from').alias('user_id'),
            F.explode(F.col('event.tags')).alias('tag'),
        )

        _reaction_tags = _reactions.select(
            F.col('event.reaction_from').alias('user_id'),
            F.col('event.message_id').alias('message_id'),
            F.col('event.reaction_type').alias('reaction_type'),
        ).join(_all_message_tags.select('message_id', 'tag'), 'message_id')

        _reaction_tops = (
            _reaction_tags.groupBy('user_id', 'tag', 'reaction_type')
            .agg(F.count('*').alias('tag_count'))
            .withColumn(
                'rank',
                F.row_number().over(
                    Window.partitionBy('user_id', 'reaction_type').orderBy(
                        F.desc('tag_count'), F.desc('tag')
                    )
                ),
            )
            .where('rank <= 3')
            .groupBy('user_id', 'reaction_type')
            .pivot('rank', [1, 2, 3])
            .agg(F.first('tag'))
            #.cache()
        )

        _like_tops = (
            _reaction_tops.where("reaction_type = 'like'")
            .drop('reaction_type')
            .withColumnRenamed('1', 'like_tag_top_1')
            .withColumnRenamed('2', 'like_tag_top_2')
            .withColumnRenamed('3', 'like_tag_top_3')
        )

        _dislike_tops = (
            _reaction_tops.where("reaction_type = 'dislike'")
            .drop('reaction_type')
            .withColumnRenamed('1', 'dislike_tag_top_1')
            .withColumnRenamed('2', 'dislike_tag_top_2')
            .withColumnRenamed('3', 'dislike_tag_top_3')
        )

        _result = _like_tops.join(_dislike_tops, 'user_id', 'full_outer')

        return _result

    _posts_part = (
        _spark.read
        .option('basePath', _events_base_path)
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
        .option('basePath', _events_base_path)
        .parquet(*_input_event_paths(_events_base_path, _date, _days_count))
        .where("event_type = 'reaction'")
    )

    _a_result = _calculate_user_interests(_posts_part, _posts_all, _reactions_part)
    _a_result.write.mode('overwrite').parquet(f'{_output_base_path}/date={_date}')

# генерируем данные для проверки
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime

_spark = SparkSession.builder.master("local").getOrCreate()

def _get_contacts(_direct_messages):
    return (
        _direct_messages.select(
            F.col("event.message_from").alias("from"),
            F.col("event.message_to").alias("to"),
            F.explode(
                F.array(F.col("event.message_from"), F.col("event.message_to"))
            ).alias("user_id"),
        )
        .withColumn(
            "contact_id",
            F.when(F.col("user_id") == F.col("from"), F.col("to")).otherwise(
                F.col("from")
            ),
        )
        .select("user_id", "contact_id")
        .distinct()
    )


def _get_contact_interests(_contacts, _interests):
    return (
        _contacts.withColumnRenamed("user_id", "u")
        .join(_interests, F.col("contact_id") == F.col("user_id"))
        .transform(lambda df: _add_tag_usage_count(df, "u"))
        .transform(lambda df: _add_tag_rank(df, "u"))
        .groupBy("u")
        .agg(*[_top_direct_tag(c) for c in _result_columns()])
        .withColumnRenamed("u", "user_id")
    )


def _get_subs_interests(_posts, _subscriptions, _verified_tags):
    _post_tags = _posts.select(
        F.col("event.message_channel_to").alias("channel_id"),
        F.col("event.tags").alias("tags"),
    )

    _verified_sub_tags = (
        _post_tags.join(
            _subscriptions,
            (F.col("event.subscription_channel") == F.col("channel_id")),
        )
        .select(
            F.col("event.user").alias("user_id"),
            F.explode(F.col("tags")).alias("tag"),
        )
        .join(_verified_tags, "tag", "left_semi")
    )

    return (
        _verified_sub_tags.groupBy("user_id", "tag")
        .agg(F.countDistinct("*").alias("tag_count"))
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.desc("tag_count"), F.desc("tag")
                )
            ),
        )
        .where("rank <= 3")
        .groupBy("user_id")
        .pivot("rank", [1, 2, 3])
        .agg(F.first("tag"))
        .withColumnRenamed("1", "sub_verified_tag_top_1")
        .withColumnRenamed("2", "sub_verified_tag_top_2")
        .withColumnRenamed("3", "sub_verified_tag_top_3")
    )


def _join_result(_contact_interests, _subs_interests):
    return _contact_interests.join(_subs_interests, "user_id", "full_outer")


def _tag_columns(_reaction):
    return [f"{_reaction}_tag_top_{X}" for X in range(1, 4)]


def _result_columns():
    return _tag_columns("like") + _tag_columns("dislike")


def _add_tag_usage_count(_df, _key):
    _res = _df
    _cols = _result_columns()
    for c in _cols:
        _res = _res.withColumn(
            c + "_count", F.count(c).over(Window.partitionBy(_key, c))
        )
    return _res


def _add_tag_rank(_df, _key):
    _res = _df
    for c in _result_columns():
        _res = _res.withColumn(
            c + "_rank",
            F.row_number().over(
                Window.partitionBy(_key).orderBy(
                    F.desc(c), F.desc(c + "_count")
                )
            ),
        )
    return _res


def _top_direct_tag(_column):
    return F.first(
        F.when(F.col(_column + "_rank") == 1, F.col(_column)), True
    ).alias("direct_" + _column)

def _input_event_paths(_base_path, _date, _depth):
    _dt = datetime.datetime.strptime(_date, "%Y-%m-%d")
    return [
        f"{_base_path}/date={(_dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}"
        for x in range(int(_depth))
    ]

_date = "2022-05-04"
_days_count = "3"
_events_base_path = "/home/student/user/USERNAME/data/events"
_interests_base_path = "/home/student/tmp/user/USERNAME/data/analytics/user_interests"
_verified_tags_path = "/home/student/user/master/data/snapshots/tags_verified/actual"

_messages = (
    _spark.read
    .option("basePath", _events_base_path)
    .parquet(*_input_event_paths(_events_base_path, _date, _days_count))
    .where("event_type='message'")
)

_direct_messages = _messages.where("event.message_to is not null")
_posts = _messages.where("event.message_channel_to is not null")

_interests = _spark.read.parquet(f"{_interests_base_path}/date={_date}")
_subscriptions = _spark.read.parquet(_events_base_path).where(f"event_type = 'subscription' and date <= '{_date}'")

_verified_tags = _spark.read.parquet(_verified_tags_path)

_contacts = _get_contacts(_direct_messages)
_contact_interests = _get_contact_interests(_contacts, _interests)#.cache()
_subs_interests = _get_subs_interests(_posts, _subscriptions, _verified_tags)#.cache()

_a_interests = join_result(_contact_interests, _subs_interests)
# сгенерировали данные для проверки

_u_interests = _spark.read.parquet(
    f"/home/student/tmp/user/USERNAME/data/analytics/connection_interests/date={_date}"
)

_test.cmp(
    _u_interests.toPandas(),
    _a_interests.toPandas(),
    m="Проверьте как формируете данные на запись.",
)