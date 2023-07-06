# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Операции с DataFrame: JOIN и UNION / Задание 4
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/4bc4223b-fc5f-4756-8581-0a863c80fc5b/theory/
# https://www.notion.so/praktikum/6-DataFrame-JOIN-UNION-81a309fc151440c38a847dbe823248c7?pvs=4#7446b956111e449c93e6b614006dd046

_test.var_exists("spark", same_type=True)

_test.var("book_1", "book_2", "columns_1", "columns_2")
_test.var_exists("df_1", "df_2", same_type=True)

_test.call("unionByName", obj=_u.df_1, in_args=[_u.df_2])
_test.call("show")

_test.output()