# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Оконные функции в PySpark / Задание 4
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/fe9c9e5e-d6ae-492d-bf10-44df096634bc/theory/
# https://www.notion.so/praktikum/9-PySpark-a59b4b96e65840a996f17d3c6593679d?pvs=4#d498baca03304af29bd0c107a8f2a437

_test.var_exists("spark", same_type=True)

_test.var("data", "columns")
_test.var_exists("df", same_type=True)

_test.call("partitionBy", in_args=["user_id"])

_test.once("withColumn", in_args=["max"])
_test.call("max", in_args=["purchase_amount"])
_test.once("over")

_test.call("withColumn", in_args=["min"])
_test.call("min", in_args=["purchase_amount"])
_test.call("over")

_test.call("select", in_args=["user_id", "max", "min"])
_test.call("distinct")
_test.call("show")

_test.output()
