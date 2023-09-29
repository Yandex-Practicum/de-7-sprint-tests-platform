# ВАЖНО: для проверки авторское выполнять не нужно
# Раскомментируйте если будете передавать решение студенту

import sys

from pyspark.sql import SparkSession


def main():
    date = sys.argv[1] # '2022-05-31'
    base_input_path = sys.argv[2] # '/home/student/tmp/user/master/data/events'
    base_output_path = sys.argv[3] # '/home/student/tmp/user/USERNAME/data/events'

    spark = (
        SparkSession
        .builder
        .master("yarn")
        .appName(f"EventsPartitioningJob-{date}")
        .getOrCreate()
    )

    # Напишите директорию чтения в общем виде
    events = spark.read.json(f"{base_input_path}/date={date}")

    # Напишите директорию записи в общем виде
    (
        events.write
        .partitionBy("event_type")
        .mode("overwrite")
        .parquet(f"{base_output_path}/date={date}")
    )


if __name__ == "__main__":
    main()