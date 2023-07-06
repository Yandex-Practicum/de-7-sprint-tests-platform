# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Знакомство со Spark / Подключение к Spark / Задача 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/0f54416f-ff31-4697-8c81-83af6fb098b5/theory/
# https://www.notion.so/praktikum/6-Spark-a25ee332403a48188022aae59b65e9c2?pvs=4#77f0093e34c74138b83560077303e396

def _user_precode():
    import warnings
    warnings.filterwarnings('ignore')

_test.imports("pyspark")
assert (
    _test.node(ast.ImportFrom).node(name="SparkSession").exists
), "Импортируйте `SparkSession` из модуля `pyspark.sql`."

_test.call("master", in_args=["local"])
_test.call("appName", in_args=["My first session"])
_test.call("getOrCreate")

_test.var_exists("spark", same_type=True)