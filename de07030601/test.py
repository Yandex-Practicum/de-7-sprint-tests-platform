# @data_tests
# DE / Организация Data Lake / Знакомство со Spark / Подключение к Spark / Задача 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/cd2511e9-f42c-4428-837d-4c7b0436062b/lessons/1561b525-4ce9-44d0-809e-4792847e1572/theory/

_test.imports("pyspark")
assert (
    _test.node(ast.ImportFrom).node(name="SparkSession").exists
), "Импортируйте `SparkSession` из модуля `pyspark.sql`."

_test.call("master", in_args=["local"])
_test.call("appName", in_args=["My first session"])
_test.call("getOrCreate")

_test.var_exists("spark", same_type=True)