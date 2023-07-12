import os

os.environ["AIRFLOW_HOME"] = "/home/student/tmp" # для запуска на кластере удалите эту строку
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

# напишите ваш код ниже
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 1, 1),
}

dag = DAG(
    dag_id="datalake_etl",
    schedule_interval=None,
    default_args=default_args,
)

# объявляем задачу с помощью BashOperator
events_partitioned = BashOperator(
    task_id="events_partitioned",
    bash_command="spark-submit --master yarn --deploy-mode cluster /lessons/partition.py 2022-05-31 /user/master/data/events /user/USERNAME/data/events",
    retries=3,
    dag=dag,
)

events_partitioned
