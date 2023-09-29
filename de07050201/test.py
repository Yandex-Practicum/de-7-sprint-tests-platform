# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/69e4b265-1ad5-4db0-b996-a5c3fe1f54b4/lessons/1867df6d-bf90-484e-bbc6-f9521fbe0ae2/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#78645ce8fa1f4594945019abd194258c

_test.imports('datetime')

_arglist = {
    0: ['2022-05-01','2019-01-28', '2001-11-20'],
    1: [1, 14, 69],
}

_test.fun_check('input_paths', _arglist)
