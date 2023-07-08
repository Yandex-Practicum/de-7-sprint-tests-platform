# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Автоматизация джобы / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9814e3a6-5fee-4a86-a7e2-cbbb1e650041/theory/
# https://www.notion.so/praktikum/12-c0f9cd8ba271422cb868d4e0867aa2f3?pvs=4#a5ac9528666d4ef0af96bd218855ccb5

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

BASH_COMMAND = [
    'spark-submit',
    '--master yarn',
    '--deploy-mode cluster',
]

_test.call('DAG')

_test.if_call([dict(name='BashOperator'), dict(name='SparkSubmitOperator')])

_test.var_exists('events_partitioned')
assert isinstance(
    _u.events_partitioned, (BashOperator, SparkSubmitOperator)
), 'Сохраните объявленную задачу в переменную `events_partitioned`.'

if _test.if_call('BashOperator'):
    for param in BASH_COMMAND:
        assert (
            param in _u.events_partitioned.bash_command
        ), f'В bash-команде не хватает параметра `{param}`.'