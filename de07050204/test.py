# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 4
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#f9774b4a8fad466da358247178fcb6db

from pyspark.sql import SparkSession as _SparkSession
from pyspark.sql import Row as _Row
from pyspark.sql.readwriter import DataFrameWriter as _DataFrameWriter

_test.call(
    'parquet',
    args=[ArgValue.Any],
    kwargs=dict(),
    m='Чтение и запись данных реализуйте методом `.parquet()`.',
)

_log_parquet_writer = [
    _e
    for _e in _log
    if _e['name'] == 'parquet' and isinstance(_e['obj'], _DataFrameWriter)
]

assert (
    len(_log_parquet_writer) == 1
), 'Проверьте что реализовали запись методом `.parquet()` один раз.'

_spark = _SparkSession.builder.config(
    'spark.ui.showConsoleProgress', 'false'
).getOrCreate()
_spark.sparkContext.setLogLevel('OFF')

try:
    _u_candidates = _spark.read.parquet(_log_parquet_writer[0]['args'][0])
except:
    assert False, (
        'Не удалось загрузить записанные данные.\n'
        'Проверьте как формируете запись.'
    )

_a_columns = ['tag', 'suggested_count']
_a_collect = [
    _Row(tag='tag_4', suggested_count=360),
    _Row(tag='tag_5', suggested_count=300),
]
_test.cmp(
    _u_candidates.columns,
    _a_columns,
    m='Проверьте как формируете порядок и названия столбцов.',
)

_test.cmp(
    _u_candidates.collect(),
    _a_collect,
    m="Проверьте как формируете данные на запись.",
)