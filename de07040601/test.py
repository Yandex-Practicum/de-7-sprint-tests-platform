# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Операции с DataFrame: JOIN и UNION / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/568d8213-d817-4909-aed0-9d20dc10f279/lessons/1a002fc5-b032-4bfa-ad78-c07312c20740/theory/

_test.var_exists("spark", same_type=True)

_test.var("book", "library", "columns", "columns_library")
_test.var_exists("df", "df_library", same_type=True)

_test.call("join", obj=_u.df, in_args=[_u.df_library, ["book_id"], "leftanti"])
_test.call("select", in_args=["title"])
_test.call("show", in_args=[10, False])

_test.output()