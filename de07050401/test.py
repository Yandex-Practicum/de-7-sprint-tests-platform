# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 3: портрет контактов пользователя / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc2f2b3-045c-4c23-b6bb-a1c85d302074/theory/
# https://www.notion.so/praktikum/4-3-cbbe0f4e30d74f0293bda436599a4ab6?pvs=4#62bb003fedb245bbb493e4cc8d696f90

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
    'direct_like_tag_top_1',
    'direct_like_tag_top_2',
    'direct_like_tag_top_3',
    'direct_dislike_tag_top_1',
    'direct_dislike_tag_top_2',
    'direct_dislike_tag_top_3',
    'sub_verified_tag_top_1',
    'sub_verified_tag_top_2',
    'sub_verified_tag_top_3',
]
_a_result_collect = [
    _Row(
        user_id='0',
        direct_like_tag_top_1=None,
        direct_like_tag_top_2=None,
        direct_like_tag_top_3=None,
        direct_dislike_tag_top_1=None,
        direct_dislike_tag_top_2=None,
        direct_dislike_tag_top_3=None,
        sub_verified_tag_top_1='tag_4',
        sub_verified_tag_top_2='tag_3',
        sub_verified_tag_top_3='tag_1',
    ),
    _Row(
        user_id='1',
        direct_like_tag_top_1='tag_A',
        direct_like_tag_top_2='tag_1',
        direct_like_tag_top_3='tag_0',
        direct_dislike_tag_top_1='tag_A',
        direct_dislike_tag_top_2='tag_3',
        direct_dislike_tag_top_3='tag_2',
        sub_verified_tag_top_1='tag_4',
        sub_verified_tag_top_2='tag_3',
        sub_verified_tag_top_3='tag_1',
    ),
    _Row(
        user_id='2',
        direct_like_tag_top_1='tag_A',
        direct_like_tag_top_2='tag_0',
        direct_like_tag_top_3='tag_3',
        direct_dislike_tag_top_1='tag_A',
        direct_dislike_tag_top_2='tag_0',
        direct_dislike_tag_top_3='tag_2',
        sub_verified_tag_top_1=None,
        sub_verified_tag_top_2=None,
        sub_verified_tag_top_3=None,
    ),
    _Row(
        user_id='3',
        direct_like_tag_top_1='tag_A',
        direct_like_tag_top_2='tag_1',
        direct_like_tag_top_3='tag_3',
        direct_dislike_tag_top_1='tag_A',
        direct_dislike_tag_top_2='tag_3',
        direct_dislike_tag_top_3='tag_0',
        sub_verified_tag_top_1='tag_4',
        sub_verified_tag_top_2='tag_3',
        sub_verified_tag_top_3='tag_1',
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
