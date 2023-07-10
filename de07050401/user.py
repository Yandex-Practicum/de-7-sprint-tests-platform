import sys

from pyspark.sql import SparkSession


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    events_base_path = sys.argv[3]
    interests_base_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    output_base_path = sys.argv[6]

    spark = (
        SparkSession.builder
        .master("local")
        .appName(f"ConnectionInterestsJob-{date}-d{days_count}")
        .getOrCreate()
    )

    # напишите ваш код ниже



if __name__ == "__main__":
    main()
