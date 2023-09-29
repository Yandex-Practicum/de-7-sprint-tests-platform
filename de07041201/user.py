import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

# напишите ваш код ниже
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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

# либо объявляем задачу с помощью SparkSubmitOperator

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# events_partitioned = SparkSubmitOperator(
#     task_id="events_partitioned",
#     dag=dag,
#     application="/lessons/partition.py",
#     conn_id="yarn_spark",
#     application_args=[
#         "2022-05-31",
#         "/user/master/data/events",
#         "/user/USERNAME/analitics/events",
#     ],
#     conf={"spark.driver.maxResultSize": "20g"},
#     executor_cores=1,
#     executor_memory="1g",
# )

events_partitioned