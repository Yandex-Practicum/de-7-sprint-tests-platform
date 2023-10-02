import sys

from pyspark.sql import SparkSession


def main():
    date = sys.argv[1] # '2022-05-25'
    days_count = sys.argv[2] # '7'
    events_base_path = sys.argv[3] # '/home/student/tmp/user/USERNAME/data/events'
    output_base_path = sys.argv[4] # '/home/student/tmp/user/USERNAME/analytics/user_interests_d7'

    spark = (
        SparkSession
        .builder
        .master("yarn")
        .appName(f"UserInterestsJob-{date}-d{days_count}")
        .getOrCreate()
    )

    # напишите ваш код ниже
    ...

    # напишите директорию записи в общем виде
    result.write...

if __name__ == "__main__":
    main()