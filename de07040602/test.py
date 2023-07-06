# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Операции с DataFrame: JOIN и UNION / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/4bc4223b-fc5f-4756-8581-0a863c80fc5b/theory/
# https://www.notion.so/praktikum/6-DataFrame-JOIN-UNION-81a309fc151440c38a847dbe823248c7?pvs=4#cc589e5e559641d5a8f1c3821dcae391

def _user_precode():
    import warnings
    warnings.filterwarnings('ignore')

_test.var_exists("spark", same_type=True)

_test.var("book", "library", "columns", "columns_library")
_test.var_exists("df", "df_library", same_type=True)

_test.call("join", obj=_u.df, in_args=[_u.df_library, ["book_id"], "leftanti"])
_test.call("select", in_args=["title"])
_test.call("count")

_test.output()