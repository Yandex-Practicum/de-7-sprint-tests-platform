# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#1746a6cc078c4161a315f95e181f49c4

_test.var_exists("result", same_type=True)

assert (
    _u.result.columns == _a.result.columns
), (
    f"Проверьте данные в переменной `result`.\n"
    f"Порядок и название столбцов должен быть такой:\n"
    f"{', '.join(f'`{x}`' for x in _a.result.columns)}."
)

assert not _u.result.subtract(_a.result).count(),  "Проверьте данные в переменной `result`."
assert not _a.result.subtract(_u.result).count(),  "Проверьте данные в переменной `result`."


_test.cmp(
    _u.result.toPandas(),
    _a.result.toPandas(),
    m="Проверьте данные в переменной `result`.",
)
