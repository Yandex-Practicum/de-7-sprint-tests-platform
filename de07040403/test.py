# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Создание DataFrame и базовые операции / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/cd2511e9-f42c-4428-837d-4c7b0436062b/lessons/1561b525-4ce9-44d0-809e-4792847e1572/theory/
# https://www.notion.so/praktikum/4-DataFrame-bffcf9bdacba49479b2339013dff1749?pvs=4#57e09de023374816bdb4ab0e05226ae9

from pyspark.sql.session import SparkSession as _SparkSession
from pyspark.sql.dataframe import DataFrame as _DataFrame

_test.var_exists('spark', 'df', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)
assert isinstance(_u.df, _DataFrame), (
    'Неверный тип переменной `df`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

msg ='Запись и чтение данных реализуйте методом `.parquet()`.'

_test.once('parquet', m=msg)
_test.once('parquet', m=msg)

_test.call('partitionBy', in_args=['channel_type'])
_test.call('mode', in_args=['append'])

_test.call('select', in_args=['channel_type'])
_test.call('orderBy', in_args=['channel_type'])
_test.call('distinct')
_test.call('show', args=[], kwargs=dict())

_a_output = \
'''+------------+
|channel_type|
+------------+
|       river|
|     channel|
+------------+'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
