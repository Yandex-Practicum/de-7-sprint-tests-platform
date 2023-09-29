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
_date = '2022-05-25'

_message_cnt = 1
_message_id = 1000
_posts = []
_reactions = []

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


_date = _datetime.strptime(_date, '%Y-%m-%d')
_pre_date = _date - _timedelta(days=_depth)
_post_date = _date + _timedelta(days=_depth)

for _dt in (_pre_date, _date, _post_date):
    for _channel_id, _admin_ids, _fan_id, _hater_id in (
            (100, [10000], 10000, 30000),
            (200, [20000, 0], 10000, 30000),
            (300, [30000, 0], 10000, 20000),
        ):
            for _admin_id in _admin_ids:
                for _i in range(1, _message_cnt + 1):
                    _message_id = _message_id + 1

                    if _admin_id == 0:
                        _tags = ['tag_0']
                    else:
                        _tags = [f'tag_{_x}' for _x in range(_admin_id, _admin_id + _i)]

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

                    if _dt <= _date:
                        _reaction_dt = _date
                    else:
                        _reaction_dt = _dt

                    _reactions.append(
                        _create_row(
                            datetime=str(_reaction_dt),
                            message_id=_message_id,
                            reaction_from=str(_fan_id),
                            reaction_type='like',
                            event_type='reaction',
                            date=_reaction_dt.date(),
                        )
                    )
                    _reactions.append(
                        _create_row(
                            datetime=str(_reaction_dt),
                            message_id=_message_id,
                            reaction_from=str(_hater_id),
                            reaction_type='dislike',
                            event_type='reaction',
                            date=_reaction_dt.date(),
                        )
                    )

_message_id = _message_id + 1
_posts.append(
    _create_row(
        admins=_admin_ids,
        channel_id=_channel_id,
        datetime=str(_date),
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
        date=_date.date(),
    )
)
_reactions.append(
    _create_row(
        datetime=str(_date),
        message_id=_message_id,
        reaction_from=str(_fan_id),
        reaction_type='like',
        event_type='reaction',
        date=_date.date(),
    )
)

_result = _posts + _reactions

_df = _DataFrame.from_dict(_result)

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
