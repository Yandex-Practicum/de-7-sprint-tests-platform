# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 4
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#22d9b9576ebf43e5a968ca3b42f9007d

def _user_precode():
    global __name__
    __name__ = "__main__"
    import sys

    sys.argv = [
        '',
        '2022-05-31',
        '5',
        '300',
        '/home/student/user/USERNAME/data/events',
        '/home/student/user/master/data/snapshots/tags_verified/actual',
        '/home/student/tmp/user/USERNAME/data/analytics/verified_tags_candidates',
    ]

# генерируем данные для сравнения
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime

def _find_candidates(_messages, _verified_tags, _suggested_cutoff):
    _all_tags = (
        _messages
        .where("event.message_channel_to is not null")
        .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])
        .groupBy("tag")
        .agg(F.countDistinct("user").alias("suggested_count"))
        .where(F.col("suggested_count") >= _suggested_cutoff)
    )

    return _all_tags.join(_verified_tags, "tag", "left_anti")

_spark = SparkSession.builder.master("local").getOrCreate()

_date = "2022-05-31"
_days_count = "5"
_suggested_cutoff = "300"
_base_input_path = "/home/student/user/USERNAME/data/events"
_verified_tags_path = "/home/student/user/master/data/snapshots/tags_verified/actual"

_dt = datetime.datetime.strptime(_date, "%Y-%m-%d")

_paths = [
    f"{_base_input_path}/date={(_dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
    for x in range(int(_days_count))
]

_messages = _spark.read.parquet(*_paths)

_verified_tags = _spark.read.parquet(_verified_tags_path)

_a_candidates = _find_candidates(_messages, _verified_tags, _suggested_cutoff)
# сгенерировали данные для сравнения

_u_candidates = _spark.read.parquet(
    f"/home/student/tmp/user/USERNAME/data/analytics/verified_tags_candidates/date={_date}"
)

_test.cmp(
    _u_candidates.toPandas(),
    _a_candidates.toPandas(),
    m="Проверьте как формируете данные на запись.",
)