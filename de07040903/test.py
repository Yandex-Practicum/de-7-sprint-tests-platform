# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Оконные функции в PySpark / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/fe9c9e5e-d6ae-492d-bf10-44df096634bc/theory/
# https://www.notion.so/praktikum/9-PySpark-a59b4b96e65840a996f17d3c6593679d?pvs=4#6783a855f6a54ce48a761d4e7aa96435

_test.var_exists("spark", "events", same_type=True)

_test.call("partitionBy", in_args=["event.message_from"])
_test.once("orderBy", in_args=["event.message_from"])

_test.call("withColumn", in_args=["lag_7"])
_test.call("lag", in_args=["event.message_to", 7])
_test.call("over")

_test.call("select", in_args=["event.message_from", "lag_7"])
_test.once("filter")
_test.call("isNotNull")
_test.call("orderBy")
_test.once("col", in_args=["event.message_to"])
_test.call("desc")
_test.call("show", in_args=[10, False])

_test.output()
