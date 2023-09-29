# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Оконные функции в PySpark / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/fe9c9e5e-d6ae-492d-bf10-44df096634bc/theory/
# https://www.notion.so/praktikum/9-PySpark-a59b4b96e65840a996f17d3c6593679d?pvs=4#55d7655fb6cb4d1b9daf2f0b60abb6a0

from pyspark.sql.session import SparkSession as _SparkSession
from pyspark.sql.dataframe import DataFrame as _DataFrame

_test.var_exists('spark', 'events', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)
assert isinstance(_u.events, _DataFrame), (
    'Неверный тип переменной `events`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

_test.call('printSchema')

_a_output = \
'''root
 |-- event: struct (nullable = true)
 |    |-- admins: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- channel_id: long (nullable = true)
 |    |-- datetime: string (nullable = true)
 |    |-- media: struct (nullable = true)
 |    |    |-- media_type: string (nullable = true)
 |    |    |-- src: string (nullable = true)
 |    |-- message: string (nullable = true)
 |    |-- message_channel_to: long (nullable = true)
 |    |-- message_from: long (nullable = true)
 |    |-- message_group: long (nullable = true)
 |    |-- message_id: long (nullable = true)
 |    |-- message_to: long (nullable = true)
 |    |-- message_ts: string (nullable = true)
 |    |-- reaction_from: string (nullable = true)
 |    |-- reaction_type: string (nullable = true)
 |    |-- subscription_channel: long (nullable = true)
 |    |-- tags: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- user: string (nullable = true)
 |-- event_type: string (nullable = true)'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
