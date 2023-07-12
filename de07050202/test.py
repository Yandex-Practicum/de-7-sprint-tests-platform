# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#871a5dc406814b49a648a9c3ad4c4505

_test.call("input_paths", in_args=["2022-05-31", 7])

_test.var_exists("candidates", same_type=True)

_test.cmp(
    _u.candidates.toPandas(),
    _a.candidates.toPandas(),
    m="Проверьте данные в переменной `candidates`.",
)
