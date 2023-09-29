# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Собираем джобу / Задание 2
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/568d8213-d817-4909-aed0-9d20dc10f279/lessons/41d18a24-ab84-47fe-acd0-050b59cd1608/theory/
# https://www.notion.so/praktikum/10-e73390e02b8c4494af2a28fb2c68f88f?pvs=4#bbf9a30558994a2582be5bbecdca0eb6

from pyspark.sql import SparkSession as _SparkSession
from pyspark.sql import Row as _Row
from pyspark.sql.readwriter import DataFrameWriter as _DataFrameWriter

_test.call(
    'json',
    args=[ArgValue.Any],
    kwargs=dict(),
    m='Чтение данных реализуйте методом `.json()`.',
)
_test.call(
    'parquet',
    args=[ArgValue.Any],
    kwargs=dict(),
    m='Запись данных реализуйте методом `.parquet()`.',
)

_log_parquet_writer = [
    _e for _e in _log
    if _e['name'] == 'parquet'
    and isinstance(_e['obj'], _DataFrameWriter)
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
    _u_events = _spark.read.parquet(_log_parquet_writer[0]['args'][0])
except:
    assert False, (
        'Не удалось загрузить записанные данные.\n'
        'Проверьте как формируете запись.'
    )
_a_columns = ['event', 'event_type']
_a_head_5 = [
    _Row(
        event=_Row(
            admins=None,
            channel_id=None,
            datetime='2022-05-31 12:12:12',
            media=None,
            message=None,
            message_channel_to=None,
            message_from=None,
            message_group=None,
            message_id=None,
            message_to=None,
            message_ts=None,
            reaction_from=None,
            reaction_type=None,
            subscription_channel=1001,
            subscription_user=None,
            tags=None,
            user='101',
        ),
        event_type='subscription',
    ),
    _Row(
        event=_Row(
            admins=None,
            channel_id=None,
            datetime='2022-05-31 13:13:23',
            media=None,
            message=None,
            message_channel_to=None,
            message_from=None,
            message_group=None,
            message_id=None,
            message_to=None,
            message_ts=None,
            reaction_from=None,
            reaction_type=None,
            subscription_channel=1001,
            subscription_user=None,
            tags=None,
            user='103',
        ),
        event_type='subscription',
    ),
    _Row(
        event=_Row(
            admins=None,
            channel_id=None,
            datetime='2022-05-31 14:14:34',
            media=None,
            message=None,
            message_channel_to=None,
            message_from=None,
            message_group=None,
            message_id=None,
            message_to=None,
            message_ts=None,
            reaction_from=None,
            reaction_type=None,
            subscription_channel=1001,
            subscription_user=None,
            tags=None,
            user='106',
        ),
        event_type='subscription',
    ),
    _Row(
        event=_Row(
            admins=None,
            channel_id=None,
            datetime='2022-05-31 15:15:45',
            media=None,
            message=None,
            message_channel_to=None,
            message_from=None,
            message_group=None,
            message_id=None,
            message_to=None,
            message_ts=None,
            reaction_from=None,
            reaction_type=None,
            subscription_channel=1001,
            subscription_user=None,
            tags=None,
            user='110',
        ),
        event_type='subscription',
    ),
    _Row(
        event=_Row(
            admins=None,
            channel_id=None,
            datetime='2022-05-31 13:23:13',
            media=None,
            message=None,
            message_channel_to=None,
            message_from=None,
            message_group=None,
            message_id=None,
            message_to=None,
            message_ts=None,
            reaction_from=None,
            reaction_type=None,
            subscription_channel=1003,
            subscription_user=None,
            tags=None,
            user='101',
        ),
        event_type='subscription',
    ),
]

_test.cmp(
    _u_events.columns,
    _a_columns,
    m='Проверьте как формируете порядок и названия столбцов.',
)

_test.cmp(
    _u_events.limit(5).collect(),
    _a_head_5,
    m="Проверьте как формируете данные на запись.",
)
