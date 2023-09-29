# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#3e55ba82221048acb294e5481bf3ba7b

from pyspark.sql import Row as _Row
from pyspark.sql.dataframe import DataFrame as _DataFrame
from pyspark.sql.session import SparkSession as _SparkSession

assert not _test.if_call('stop'), 'Не используйте `stop` в вашем решении.'

_test.var_exists('spark', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

_test.fun_exists('reaction_tag_tops')

_test.call('reaction_tag_tops', args=['2022-05-25', 7, _u.spark])

_test.var_exists('result', precode=True)

assert isinstance(_u.result, _DataFrame), (
    'Неверный тип переменной `result`.\n'
    'Результирующий датафрейм сохраните в переменной `result`. '
)

_a_result = [
    _Row(
        user_id='1',
        like_tag_top_1='tag_A',
        like_tag_top_2='tag_0',
        like_tag_top_3='tag_3',
        dislike_tag_top_1=None,
        dislike_tag_top_2=None,
        dislike_tag_top_3=None,
    ),
    _Row(
        user_id='2',
        like_tag_top_1='tag_A',
        like_tag_top_2='tag_1',
        like_tag_top_3='tag_0',
        dislike_tag_top_1='tag_A',
        dislike_tag_top_2='tag_3',
        dislike_tag_top_3='tag_0',
    ),
    _Row(
        user_id='3',
        like_tag_top_1=None,
        like_tag_top_2=None,
        like_tag_top_3=None,
        dislike_tag_top_1='tag_A',
        dislike_tag_top_2='tag_0',
        dislike_tag_top_3='tag_2',
    ),
    _Row(
        user_id='4',
        like_tag_top_1='tag_3',
        like_tag_top_2='tag_4',
        like_tag_top_3='tag_2',
        dislike_tag_top_1=None,
        dislike_tag_top_2=None,
        dislike_tag_top_3=None,
    ),
]

_test.cmp(
    _u.result.limit(5).collect(),
    _a_result,
    m='Проверьте данные в переменной `result`.',
)
