from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from os import makedirs as _makedirs
from shutil import rmtree as _rmtree

from pandas import DataFrame as _DataFrame
from pyspark.sql import SparkSession as _SparkSession

_user_path = '/home/student/tmp/user'
_events_path = '/home/student/tmp/user/USERNAME/data/events'
_tags_verified_path = '/home/student/tmp/user/master/data/snapshots/tags_verified/actual'

_depth = 84
_date = '2022-05-31'

_message_cnt = 1
_message_id = 1000

_posts = []
_reactions = []
_direct_messages = []
_subscriptions = []
_group_messages = []
_tags_verified = []

_rmtree(_user_path, ignore_errors=True)

_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')


def _input_paths(date, depth):
    dt = _datetime.strptime(date, '%Y-%m-%d')
    return [
        f'/home/student/tmp/user/USERNAME/data/events/date={(dt-_timedelta(days=days)).strftime("%Y-%m-%d")}/event_type=message'
        for days in range(depth)
    ]

_paths = _input_paths(_date, _depth)

for _path in _paths:
    _makedirs(_path, exist_ok=True)

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
    subscription_channel=0,
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
    for _channel_id, _admin_ids in tuple(
        (_id, list(range(_id, _id + 3)))
        for _id in range(1, 300, 3)
    ):
        for _admin_id in _admin_ids:
            for _i in range(1, _message_cnt + 1):
                _message_id = _message_id + 1

                if _admin_id % 10 == 0:
                    _tags = ['tag_0']
                else:
                    _tags = [f'tag_{_x}' for _x in range(1, _admin_id % 10 + 1)]

                _posts.append(
                    _create_row(
                        admins=_admin_ids,
                        channel_id=_channel_id,
                        datetime=str(_dt),
                        media=dict(
                            media_type='image',
                            src=f'https://media-for-message-id-{_message_id}...',
                        ),
                        message=f'Message {_message_id} from user {_admin_id} to channel {_channel_id}...',
                        message_channel_to=_channel_id,
                        message_from=_admin_id,
                        message_id=_message_id,
                        tags=_tags,
                        event_type='message',
                        date=_dt.date(),
                    )
                )

for _user_ids in tuple(
    (n1, n2)
    for n1 in (10000, 20000, 30000)
    for n2 in (10000, 20000, 30000)
    if n1 != n2
):
    _message_id = _message_id + 1
    _direct_messages.append(
        _create_row(
            message=f'Message {_message_id} from user {_user_ids[0]} to user {_user_ids[1]}...',
            message_from=_user_ids[0],
            message_to=_user_ids[1],
            message_ts=str(_date),
            event_type='message',
            date=_date.date(),
        )
    )

for _user_ids in tuple(
    (n1, n2)
    for n1 in (10000, 20000, 30000)
    for n2 in (0,)
):
    _message_id = _message_id + 1
    _direct_messages.append(
        _create_row(
            message=f'Message {_message_id} from user {_user_ids[0]} to user {_user_ids[1]}...',
            message_from=_user_ids[0],
            message_to=_user_ids[1],
            message_ts=str(_post_date),
            event_type='message',
            date=_post_date.date(),
        )
    )

for _channel_id in (100, 200, 300):
    for _user_id in (10000, 30000, 0):
        _subscriptions.append(
            _create_row(
                datetime=str(_pre_date),
                subscription_channel=_channel_id,
                user=str(_user_id),
                event_type='subscription',
                date=_pre_date.date(),
            )
        )

_message_id = _message_id + 1
_group_messages.append(
    _create_row(
        message=f'Message {_message_id} from user {1} to group {123} to {2}...',
        message_from=1,
        message_group=123,
        message_id=_message_id,
        message_to=2,
        message_ts=str(_date),
        event_type='message',
        date=_date.date(),
    )
)

_result = _posts + _reactions + _direct_messages + _subscriptions + _group_messages

_df = _DataFrame.from_dict(_result)

_df.to_parquet(_events_path, partition_cols=['date', 'event_type'])

def _create_tags_verified(tag=None):
    result = {'tag': tag}
    return result

for _tag in ('tag_0', 'tag_1', 'tag_2', 'tag_3'):
    _tags_verified.append(_create_tags_verified(_tag))

_df = _DataFrame.from_dict(_tags_verified)

_makedirs(_tags_verified_path, exist_ok=True)

_df.to_parquet(f'{_tags_verified_path}/tags_verified.parquet')