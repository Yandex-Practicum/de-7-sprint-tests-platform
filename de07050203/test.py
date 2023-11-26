# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#c10556aa8d644dbcb816c925d8d77ddc

from pyspark.sql import Row as _Row
from pyspark.sql.dataframe import DataFrame as _DataFrame
from pyspark.sql.session import SparkSession as _SparkSession

assert not _test.if_call('stop'), 'Не используйте `stop` в вашем решении.'

_test.var_exists('spark', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

_arglist = {
    0: ['2022-05-01', '2019-01-28', '2001-11-20'],
    1: [1, 14, 84],
}

_test.fun_check('input_paths', _arglist, precode=True)

_test.call('input_paths', in_args=['2022-05-31', 84])

_test.var_exists('candidates', precode=True)

assert isinstance(_u.candidates, _DataFrame), (
    'Неверный тип переменной `candidates`.\n'
    'Результирующий датафрейм сохраните в переменной `candidates`. '
)

_a_candidates = [
    _Row(tag='tag_6', suggested_count=120),
    _Row(tag='tag_4', suggested_count=180),
    _Row(tag='tag_5', suggested_count=150),
]

_test.cmp(
    _u.candidates.collect(),
    _a_candidates,
    m='Проверьте данные в переменной `candidates`.',
)