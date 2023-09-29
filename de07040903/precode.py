from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from os import makedirs as _makedirs
from shutil import rmtree as _rmtree

from pandas import DataFrame as _DataFrame
from pandas import to_datetime as _to_datetime
from pyspark.sql import SparkSession as _SparkSession

_user_path = '/home/student/tmp/user'
_events_path = '/home/student/tmp/user/master/data/events'

_depth = 1
_date = '2022-05-01'

_message_id = 1000

_direct_messages = []

_rmtree(_user_path, ignore_errors=True)

_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')


def _create_row(
    admins=None,
    channel_id=None,
    datetime=None,
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
    subscription_channel=None,
    tags=None,
    user=None,
    date=None,
    event_type=None,
):
    result = {
        'event': {
            'admins': admins,
            'channel_id': channel_id,
            'datetime': datetime,
            'media': media,
            'message': message,
            'message_channel_to': message_channel_to,
            'message_from': message_from,
            'message_group': message_group,
            'message_id': message_id,
            'message_to': message_to,
            'message_ts': message_ts,
            'reaction_from': reaction_from,
            'reaction_type': reaction_type,
            'subscription_channel': subscription_channel,
            'tags': tags,
            'user': user,
        },
        'date': date,
        'event_type': event_type,
    }

    return result

_date = _datetime.strptime(_date, '%Y-%m-%d')
_pre_date = _date - _timedelta(days=_depth)
_post_date = _date + _timedelta(days=_depth)

for _dt in (_pre_date, _date, _post_date):
    for _uid_from in range(1001, 1011):
        for _uid_to in range(1, 11):
            _message_id = _message_id + 1
            _uid_to = _uid_to * _uid_from + _uid_to
            _direct_messages.append(
                _create_row(
                    message=f'Message {_message_id} from user {_uid_from} to user {_uid_to}...',
                    message_from=_uid_from,
                    message_to=_uid_to,
                    message_ts=str(_dt),
                    event_type='message',
                    date=_dt.date(),
                )
            )

_df = _DataFrame.from_dict(_direct_messages)

_paths = [
    f'{_events_path}/date={_date}'
    for _date in _to_datetime(_df['date']).dt.date.unique()
]

for _path in _paths:
    _makedirs(_path, exist_ok=True)

for _date, _group in _df.groupby('date'):
    _group.drop('date', axis=1).to_json(
        f'{_events_path}/date={_date}/data-{_date}.json',
        orient='records',
        lines=True,
    )
