# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 5
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#ce9d1dff0b614b6194297746ee4ff4d8

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

_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')

try:
    _u_result = _spark.read.parquet(_log_parquet_writer[0]['args'][0])
except:
    assert False, (
        'Не удалось загрузить записанные данные.\n'
        'Проверьте как формируете запись.'
    )

_a_result_columns = [
    'user_id',
    'tag_top_1',
    'tag_top_2',
    'tag_top_3',
    'like_tag_top_1',
    'like_tag_top_2',
    'like_tag_top_3',
    'dislike_tag_top_1',
    'dislike_tag_top_2',
    'dislike_tag_top_3',
]
_a_result_collect = [
    _Row(
        user_id='0',
        tag_top_1='tag_A',
        tag_top_2='tag_0',
        tag_top_3=None,
        like_tag_top_1=None,
        like_tag_top_2=None,
        like_tag_top_3=None,
        dislike_tag_top_1=None,
        dislike_tag_top_2=None,
        dislike_tag_top_3=None,
    ),
    _Row(
        user_id='1',
        tag_top_1='tag_1',
        tag_top_2='tag_2',
        tag_top_3='tag_3',
        like_tag_top_1='tag_A',
        like_tag_top_2='tag_0',
        like_tag_top_3='tag_3',
        dislike_tag_top_1=None,
        dislike_tag_top_2=None,
        dislike_tag_top_3=None,
    ),
    _Row(
        user_id='2',
        tag_top_1='tag_2',
        tag_top_2='tag_3',
        tag_top_3='tag_4',
        like_tag_top_1='tag_A',
        like_tag_top_2='tag_1',
        like_tag_top_3='tag_0',
        dislike_tag_top_1='tag_A',
        dislike_tag_top_2='tag_3',
        dislike_tag_top_3='tag_0',
    ),
    _Row(
        user_id='3',
        tag_top_1='tag_3',
        tag_top_2='tag_4',
        tag_top_3='tag_5',
        like_tag_top_1=None,
        like_tag_top_2=None,
        like_tag_top_3=None,
        dislike_tag_top_1='tag_A',
        dislike_tag_top_2='tag_0',
        dislike_tag_top_3='tag_2',
    ),
    _Row(
        user_id='4',
        tag_top_1=None,
        tag_top_2=None,
        tag_top_3=None,
        like_tag_top_1='tag_3',
        like_tag_top_2='tag_4',
        like_tag_top_3='tag_2',
        dislike_tag_top_1=None,
        dislike_tag_top_2=None,
        dislike_tag_top_3=None,
    ),
]
_test.cmp(
    _u_result.columns,
    _a_result_columns,
    m='Проверьте как формируете порядок и названия столбцов.',
)

_test.cmp(
    _u_result.limit(5).collect(),
    _a_result_collect,
    m="Проверьте как формируете данные на запись.",
)
