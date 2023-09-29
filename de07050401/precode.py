global __name__
__name__ = '__main__'

import sys as _sys
from datetime import datetime as _datetime
from datetime import timedelta as _timedelta
from os import makedirs as _makedirs
from shutil import rmtree as _rmtree

from pandas import DataFrame as _DataFrame
from pyspark.sql import SparkSession as _SparkSession

_WORKDIR = '/home/student/tmp'
_DATE = '2022-05-25'
_DEPTH = '7'
_MESSAGE_CNT = 5

_message_id = 1000
_posts = []
_reactions = []
_direct_messages = []
_subscriptions = []
_group_messages = []

_user_path = f'{_WORKDIR}/user'
_events_base_path = f'{_WORKDIR}/user/USERNAME/data/events'
_user_interests_base_path = f'{_WORKDIR}/user/USERNAME/analytics/user_interests_d{_DEPTH}'
_verified_tags_path = f'{_WORKDIR}/user/master/data/snapshots/tags_verified/actual'
_output_base_path = f'{_WORKDIR}/user/USERNAME/data/analytics/connection_interests_d{_DEPTH}'

_sys.argv = [
    'connection_interests.py',
    _DATE,
    _DEPTH,
    _events_base_path,
    _user_interests_base_path,
    _verified_tags_path,
    _output_base_path
]

_rmtree(_user_path, ignore_errors=True)

_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')

def _input_events_paths(base_path, date, depth, event_type):
    dt = _datetime.strptime(date, '%Y-%m-%d')
    return [
        f'{base_path}/date={(dt-_timedelta(days=_day)).strftime("%Y-%m-%d")}/event_type={event_type}'
        for _day in range(int(depth))
    ]

for _event_type in ('message', 'reaction', 'subscription'):
    _paths = _input_events_paths(_events_base_path, _DATE, _DEPTH, _event_type)
    for _path in _paths:
        _makedirs(_path, exist_ok=True)

_makedirs(f'{_user_interests_base_path}/date={_DATE}', exist_ok=True)

_makedirs(_verified_tags_path, exist_ok=True)

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
    subscription_user=None,
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
            'subscription_user': subscription_user,
            'tags': tags,
            'user': user,
        },
        'date': date,
        'event_type': event_type,
    }

    return result


_date = _datetime.strptime(_DATE, '%Y-%m-%d')
_pre_date = _date - _timedelta(days=int(_DEPTH))
_post_date = _date + _timedelta(days=int(_DEPTH))

for _dt in (_pre_date, _date, _post_date):
    for _channel_id, _admin_ids, _fan_id, _hater_id in (
        (1, [1, 0], '2', '3'),
        (2, [2, 0], '1', '3'),
        (3, [3, 0], '1', '2'),
        (4, [1, 2, 3], '4', None),
        (5, [1, 2, 3], '4', None),
    ):
        for _admin_id in _admin_ids:
            for _i in range(1, _MESSAGE_CNT + 1):
                _message_id = _message_id + 1

                if _admin_id == 0:
                    _tags = ['tag_0', 'tag_A']
                else:
                    _tags = [f'tag_{_x}' for _x in range(_admin_id, _admin_id + _i)]

                _posts.append(
                    _create_row(
                        admins=_admin_ids,
                        channel_id=_channel_id,
                        datetime=str(_dt),
                        media=dict(
                            media_type='image',
                            src=f'https://media-for-message-id-{_message_id}',
                        ),
                        message=f'Message {_message_id} from user {_admin_id} to channel {_channel_id}',
                        message_channel_to=_channel_id,
                        message_from=_admin_id,
                        message_id=_message_id,
                        tags=_tags,
                        event_type='message',
                        date=_dt.date(),
                    )
                )

                if _dt <= _date:
                    _reaction_dt = _date
                else:
                    _reaction_dt = _dt

                if _fan_id:
                    _reactions.append(
                        _create_row(
                            datetime=str(_reaction_dt),
                            message_id=_message_id,
                            reaction_from=_fan_id,
                            reaction_type='like',
                            event_type='reaction',
                            date=_reaction_dt.date(),
                        )
                    )
                if _hater_id:
                    _reactions.append(
                        _create_row(
                            datetime=str(_reaction_dt),
                            message_id=_message_id,
                            reaction_from=_hater_id,
                            reaction_type='dislike',
                            event_type='reaction',
                            date=_reaction_dt.date(),
                        )
                    )

for _user_ids in tuple(
    (n1, n2)
    for n1 in (1, 2, 3)
    for n2 in (1, 2, 3)
    if n1 != n2
):
    _message_id = _message_id + 1
    _direct_messages.append(
        _create_row(
            message=f'Message {_message_id} from user {_user_ids[0]} to user {_user_ids[1]}',
            message_from=_user_ids[0],
            message_to=_user_ids[1],
            message_ts=str(_date),
            event_type='message',
            date=_date.date(),
        )
    )

for _user_ids in tuple(
    (n1, n2)
    for n1 in (1, 2, 3)
    for n2 in (0,)
):
    _message_id = _message_id + 1
    _direct_messages.append(
        _create_row(
            message=f'Message {_message_id} from user {_user_ids[0]} to user {_user_ids[1]}',
            message_from=_user_ids[0],
            message_to=_user_ids[1],
            message_ts=str(_post_date),
            event_type='message',
            date=_post_date.date(),
        )
    )

for _channel_id in (1, 2, 3):
    for _user_id in (1, 3, 0):
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
        message=f'Message {_message_id} from user {1} to group {123} to {2}',
        message_from=1,
        message_group=123,
        message_id=_message_id,
        message_to=2,
        message_ts=str(_date),
    )
)

_result = _posts + _reactions + _direct_messages + _subscriptions + _group_messages

_DataFrame.from_dict(_result).to_parquet(_events_base_path, partition_cols=['date', 'event_type'])

_user_interests = [
    {
        'user_id': '0',
        'tag_top_1': 'tag_A',
        'tag_top_2': 'tag_0',
        'tag_top_3': None,
        'like_tag_top_1': None,
        'like_tag_top_2': None,
        'like_tag_top_3': None,
        'dislike_tag_top_1': None,
        'dislike_tag_top_2': None,
        'dislike_tag_top_3': None,
    },
    {
        'user_id': '1',
        'tag_top_1': 'tag_1',
        'tag_top_2': 'tag_2',
        'tag_top_3': 'tag_3',
        'like_tag_top_1': 'tag_A',
        'like_tag_top_2': 'tag_0',
        'like_tag_top_3': 'tag_3',
        'dislike_tag_top_1': None,
        'dislike_tag_top_2': None,
        'dislike_tag_top_3': None,
    },
    {
        'user_id': '2',
        'tag_top_1': 'tag_2',
        'tag_top_2': 'tag_3',
        'tag_top_3': 'tag_4',
        'like_tag_top_1': 'tag_A',
        'like_tag_top_2': 'tag_1',
        'like_tag_top_3': 'tag_0',
        'dislike_tag_top_1': 'tag_A',
        'dislike_tag_top_2': 'tag_3',
        'dislike_tag_top_3': 'tag_0',
    },
    {
        'user_id': '3',
        'tag_top_1': 'tag_3',
        'tag_top_2': 'tag_4',
        'tag_top_3': 'tag_5',
        'like_tag_top_1': None,
        'like_tag_top_2': None,
        'like_tag_top_3': None,
        'dislike_tag_top_1': 'tag_A',
        'dislike_tag_top_2': 'tag_0',
        'dislike_tag_top_3': 'tag_2',
    },
    {
        'user_id': '4',
        'tag_top_1': None,
        'tag_top_2': None,
        'tag_top_3': None,
        'like_tag_top_1': 'tag_3',
        'like_tag_top_2': 'tag_4',
        'like_tag_top_3': 'tag_2',
        'dislike_tag_top_1': None,
        'dislike_tag_top_2': None,
        'dislike_tag_top_3': None,
    },
]
_DataFrame.from_dict(_user_interests).to_parquet(f'{_user_interests_base_path}/date={_DATE}/user_interests.parquet')

_tags_verified = [
    {'tag': 'tag_1'},
    # {'tag': 'tag_2'}, #TODO
    {'tag': 'tag_3'},
    {'tag': 'tag_4'},
    # {'tag': 'tag_0'}, #TODO
]

_DataFrame.from_dict(_tags_verified).to_parquet(f'{_verified_tags_path}/tags_verified.parquet')