# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Операции с DataFrame: кэширование и контрольные точки / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/f5f6769a-5dc4-4c77-a8fa-09a46e5db6d1/theory/
# https://www.notion.so/praktikum/7-DataFrame-f0f40109ac0e4524a0fce21ec5fd60fb?pvs=4#8e51f160d9ff4bb8b3450d1f28eec8d1

CHECKPOINT_PATH = "/home/student/tmp/user/USERNAME/analytics/test_check"

_test.var_exists("spark", same_type=True)

_test.var("book", "library", "columns", "columns_library")
_test.var_exists("df", "df_library", "df_join", "df_cache", same_type=True)

_test.call("setCheckpointDir", in_args=[CHECKPOINT_PATH])
_test.call("checkpoint")
_test.call("explain")
