# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Встроенные стандартные функции / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/1313027e-19ef-46d9-b4da-10ea38e429bf/theory/
# https://www.notion.so/praktikum/8-3d1daaa4bf8444d7af1e275494a1a1a8?pvs=4#016743371efe4f13982cd9276c629f16

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

_test.call('hour')
_test.call('minute')
_test.call('second')

_test.call('orderBy')

_test.call([
    dict(name='show', args=[10], kwargs=dict()),
    dict(name='show', args=[10, True], kwargs=dict()),
])

_a_output = \
'''+--------------------+------------+----+------+------+
|               event|  event_type|hour|minute|second|
+--------------------+------------+----+------+------+
|{null, null, 2022...|subscription|  17|    37|    47|
|{null, null, 2022...|subscription|  16|    36|    36|
|{null, null, 2022...|subscription|  16|    26|    46|
|{null, null, 2022...|subscription|  15|    35|    25|
|{null, null, 2022...|subscription|  15|    25|    35|
|{null, null, 2022...|subscription|  15|    15|    45|
|{null, null, 2022...|subscription|  14|    34|    14|
|{null, null, 2022...|subscription|  14|    24|    24|
|{null, null, 2022...|subscription|  14|    14|    34|
|{null, null, 2022...|subscription|  13|    23|    13|
+--------------------+------------+----+------+------+
only showing top 10 rows'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
