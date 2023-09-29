from os import makedirs as _makedirs
from shutil import rmtree as _rmtree

from pandas import DataFrame as _DataFrame
from pyspark.sql import SparkSession as _SparkSession

_channel_id = 1000
_channels = []

_user_path = '/home/student/tmp/user'
_channels_path = '/home/student/tmp/user/master/data/snapshots/channels/actual'

_rmtree(_user_path, ignore_errors=True)

_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')


def _create_row(name=None, channel_type=None, id=None, image_src=None):
    result = {
        'name': name,
        'channel_type': channel_type,
        'id': id,
        'image_src': image_src,
    }

    return result


for _channel_type in ('channel', 'river'):
    for _ in range(10):
        _channel_id = _channel_id + 1
        _channels.append(
            _create_row(
                name=f'Channel {_channel_id}...',
                channel_type=_channel_type,
                id=_channel_id,
                image_src=f'https://{_channel_id}...',
            )
        )

_df = _DataFrame.from_dict(_channels)

_makedirs(_channels_path, exist_ok=True)
_df.to_parquet(f'{_channels_path}/channels.parquet')
