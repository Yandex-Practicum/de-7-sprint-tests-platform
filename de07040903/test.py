# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Оконные функции в PySpark / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/fe9c9e5e-d6ae-492d-bf10-44df096634bc/theory/
# https://www.notion.so/praktikum/9-PySpark-a59b4b96e65840a996f17d3c6593679d?pvs=4#6783a855f6a54ce48a761d4e7aa96435

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

_test.call('Window')
_test.call('isNotNull')

_test.call('show', args=[10, False])

_a_output = \
'''+------------+-----+
|message_from|lag_7|
+------------+-----+
|1010        |3033 |
|1009        |3030 |
|1008        |3027 |
|1007        |3024 |
|1006        |3021 |
|1005        |3018 |
|1004        |3015 |
|1003        |3012 |
|1002        |3009 |
|1001        |3006 |
+------------+-----+
only showing top 10 rows'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
