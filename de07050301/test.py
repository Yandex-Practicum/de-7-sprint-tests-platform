# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#504267554b314827a55bd419de03291e

from pyspark.sql import Row as _Row
from pyspark.sql.dataframe import DataFrame as _DataFrame
from pyspark.sql.session import SparkSession as _SparkSession

assert not _test.if_call('stop'), 'Не используйте `stop` в вашем решении.'

_test.var_exists('spark', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

_test.fun_exists('tag_tops')

_test.call('tag_tops', args=['2022-05-25', 7, _u.spark])

_test.var_exists('result', precode=True)

assert isinstance(_u.result, _DataFrame), (
    'Неверный тип переменной `result`.\n'
    'Результирующий датафрейм сохраните в переменной `result`. '
)

_a_result = [
    _Row(user_id=0, tag_top_1='tag_A', tag_top_2='tag_0', tag_top_3=None),
    _Row(user_id=1, tag_top_1='tag_1', tag_top_2='tag_2', tag_top_3='tag_3'),
    _Row(user_id=2, tag_top_1='tag_2', tag_top_2='tag_3', tag_top_3='tag_4'),
    _Row(user_id=3, tag_top_1='tag_3', tag_top_2='tag_4', tag_top_3='tag_5'),
]

_test.cmp(
    _u.result.limit(5).collect(),
    _a_result,
    m='Проверьте данные в переменной `result`.',
)
