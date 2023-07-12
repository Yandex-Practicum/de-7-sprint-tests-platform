# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Встроенные стандартные функции / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/1313027e-19ef-46d9-b4da-10ea38e429bf/theory/
# https://www.notion.so/praktikum/8-3d1daaa4bf8444d7af1e275494a1a1a8?pvs=4#30aed042f16a4421b703a0c37998970d

_test.var_exists("spark", "events", same_type=True)

_test.call("filter")
_test.call("col", in_args=["event.message_to"])
_test.call("isNotNull")
_test.call("count")

_test.output()