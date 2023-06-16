# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Создание DataFrame и базовые операции / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/cd2511e9-f42c-4428-837d-4c7b0436062b/lessons/1561b525-4ce9-44d0-809e-4792847e1572/theory/

LOAD_PATH = "/user/master/data/snapshots/channels/actual"
SAVE_PATH = "/user/USERNAME/analytics/test"

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