# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Создание DataFrame и базовые операции / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/e86a4e6c-0f3f-431a-8fa5-6d539a6a9825/theory/
# https://www.notion.so/praktikum/4-DataFrame-bffcf9bdacba49479b2339013dff1749?pvs=4#46dacce8574b4e35bd2eee2abb3dc7e6

from pyspark.sql.session import SparkSession as _SparkSession
from pyspark.sql.dataframe import DataFrame as _DataFrame

_test.var_exists('spark', precode=True)

assert isinstance(_u.spark, _SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

_test.var_exists('events')

assert isinstance(_u.events, _DataFrame), (
    'Неверный тип переменной `events`.\n'
    'Сохраните набор данных в переменную `events`.'
)

_test.call('show', args=[10], kwargs=dict(), obj=_u.events)

_a_output = \
'''+--------------------+------------+
|               event|  event_type|
+--------------------+------------+
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
|{null, null, 2022...|subscription|
+--------------------+------------+
only showing top 10 rows'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
