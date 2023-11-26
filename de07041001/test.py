# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Собираем джобу / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc52da1-720a-48b0-b40c-5fc485b8bdb2/theory/
# https://www.notion.so/praktikum/10-e73390e02b8c4494af2a28fb2c68f88f?pvs=4#05fe4ecd2bfa4ee3a1dd5601e4ea7d66

import pyspark as _pyspark

assert isinstance(_u.spark, _pyspark.sql.session.SparkSession), (
    'Неверный тип переменной `spark`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)
assert isinstance(_u.events, _pyspark.sql.dataframe.DataFrame), (
    'Неверный тип переменной `events`.\n'
    'Пожалуйста, не изменяйте прекод. Это важно для проверки.'
)

msg ='Запись и чтение данных реализуйте методом `.parquet()`.'

_test.once('parquet', m=msg)
_test.once('parquet', m=msg)

_test.call('desc')

_test.call('show', in_args=[10])

_a_output = \
'''+--------------------+----------+------------+
|               event|      date|  event_type|
+--------------------+----------+------------+
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
|{null, null, 2022...|2022-06-03|subscription|
+--------------------+----------+------------+
only showing top 10 rows'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)