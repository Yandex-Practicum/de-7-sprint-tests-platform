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

    # напишите директорию чтения в общем виде
    events = spark.read....

    # напишите директорию записи в общем виде
    events.write....


if __name__ == "__main__":
    main()