# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Собираем джобу / Задание 2 (TEST)
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc52da1-720a-48b0-b40c-5fc485b8bdb2/theory/
# https://www.notion.so/praktikum/10-e73390e02b8c4494af2a28fb2c68f88f?pvs=4#bbf9a30558994a2582be5bbecdca0eb6

def _user_precode():
    global __name__
    __name__ = '__main__'
    import sys

    sys.argv = [
        'partition.py',
        '2022-05-31',
        '/home/student/user/master/data/events',
        '/home/student/tmp/user/USERNAME/data/events',
    ]


from pyspark.sql import SparkSession

# проверка методов внутри джобы
_test.call(
    [
        dict(name="json"),
        dict(name="load"),
        dict(name="load", in_kwargs=dict(format="json")),
    ],
    m="Напишите директорию чтения в общем виде.",
)

_test.call("partitionBy", in_args=["event_type"])
_test.call("mode", in_args=["overwrite"])

_test.call(
    [
        dict(name="parquet"),
        dict(name="save"),
        dict(name="save", in_kwargs=dict(format="parquet")),
    ],
    m="Напишите директорию записи в общем виде.",
)

# проверка результата
_spark = SparkSession.builder.master("local").getOrCreate()

_u_result = _spark.read.parquet(
    "/home/student/tmp/user/USERNAME/data/events/date=2022-05-31"
)

_a_result = _spark.read.parquet(
    "/home/student/user/USERNAME/data/events/date=2022-05-31"
)

_test.cmp(
    _u_result.toPandas(),
    _a_result.toPandas(),
    m="Проверьте как формируете данные на запись.",
)
