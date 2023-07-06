# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Создание DataFrame и базовые операции / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/e86a4e6c-0f3f-431a-8fa5-6d539a6a9825/theory/
# https://www.notion.so/praktikum/4-DataFrame-bffcf9bdacba49479b2339013dff1749?pvs=4#57e09de023374816bdb4ab0e05226ae9

def _user_precode():
    import warnings
    warnings.filterwarnings('ignore')

LOAD_PATH = "/home/student/user/master/data/snapshots/channels/actual"
SAVE_PATH = "/home/student/tmp/user/USERNAME/analytics/test"

_test.var_exists("spark", same_type=True)

_test.once(
    [
        dict(name="parquet", in_args=[LOAD_PATH]),
        dict(
            name="load",
            in_kwargs=dict(
                path=LOAD_PATH,
                format="parquet",
            ),
        ),
    ],
    m="Не меняйте прекод.",
)

_test.call("partitionBy", in_args=["channel_type"])
_test.call("mode", in_args=["append"])

_test.once(
    [
        dict(name="parquet", in_args=[SAVE_PATH]),
        dict(name="save", in_args=[SAVE_PATH]),
    ],
    m=f"Cохраните новый датафрейм c актуальной информацией о каналах в диркеторию `{SAVE_PATH}`.",
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

_test.call("select", in_args=["channel_type"])
_test.call("orderBy", in_args=["channel_type"])
_test.call("distinct")
_test.call("show")

_test.output()