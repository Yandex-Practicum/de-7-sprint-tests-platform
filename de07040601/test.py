# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Операции с DataFrame: JOIN и UNION / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/4bc4223b-fc5f-4756-8581-0a863c80fc5b/theory/
# https://www.notion.so/praktikum/6-DataFrame-JOIN-UNION-81a309fc151440c38a847dbe823248c7?pvs=4#08fa6c4469e541ba9a324e5a28f46f1e

_test.var_exists('spark', same_type=True, precode=True)

_test.var('book', 'library', 'columns', 'columns_library', precode=True)

_test.var_exists('df', 'df_library', same_type=True, precode=True)

_test.call('join')

_test.call('show', args=[10, False])

_test.output()
