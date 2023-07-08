# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Собираем джобу / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc52da1-720a-48b0-b40c-5fc485b8bdb2/theory/
# https://www.notion.so/praktikum/10-e73390e02b8c4494af2a28fb2c68f88f?pvs=4#05fe4ecd2bfa4ee3a1dd5601e4ea7d66

from pyspark.sql import DataFrame

LOAD_PATH = "/home/student/user/master/data/events"
SAVE_PATH = "/home/student/tmp/user/USERNAME/data/events"

_test.var_exists("spark")

_test.once(
    [
        dict(name="json", in_args=[LOAD_PATH]),
        dict(
            name="load",
            in_kwargs=dict(
                path=LOAD_PATH,
                format="json",
            ),
        ),
    ],
    m=f"Не меняйте прекод для чтения данных из директории {LOAD_PATH}.",
)

assert isinstance(
    _u.events, DataFrame
), "Не меняйте значение переменной `events`."

_test.call("partitionBy", in_args=["date", "event_type"])
_test.call("mode", in_args=["overwrite"])

_test.once(
    [
        dict(name="parquet", in_args=[SAVE_PATH]),
        dict(name="save", in_args=[SAVE_PATH]),
    ],
    m=f"Cохраните сырые JSON-файлы в формат Parquet в диркеторию `{SAVE_PATH}`.",
)

_test.once(
    [
        dict(name="parquet", in_args=[SAVE_PATH]),
        dict(
            name="load",
            in_kwargs=dict(
                path=SAVE_PATH,
                format="parquet",
            ),
        ),
    ],
    m=f"Прочтите файл из директории `{SAVE_PATH}`.",
)

_test.call("orderBy")
_test.call("col", in_args=["event.datetime"])
_test.call("desc")
_test.call("show", in_args=[10])
