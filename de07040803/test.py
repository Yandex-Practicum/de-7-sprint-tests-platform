# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Встроенные стандартные функции / Задание 3
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/1313027e-19ef-46d9-b4da-10ea38e429bf/theory/
# https://www.notion.so/praktikum/8-3d1daaa4bf8444d7af1e275494a1a1a8?pvs=4#4ed8a490ed024d1ba1e957a6efceda33

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

_test.call('filter')
_test.call('groupBy')
_test.call('max')

_test.call('show', args=[], kwargs=dict())

_a_output = \
'''+----------+
|max(count)|
+----------+
|        11|
+----------+'''

_test.cmp(
    _u._output,
    _a_output,
    m='Неверный вывод. Проверьте выводимые значения.',
)
