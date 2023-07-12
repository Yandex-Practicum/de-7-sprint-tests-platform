import sys

from pyspark.sql import SparkSession


def main():
    date = sys.argv[1]
    days_count = sys.argv[2]
    suggested_cutoff = sys.argv[3]
    base_input_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    base_output_path = sys.argv[6]

    spark = (
        SparkSession.builder
        .master("local") # для запуска на кластере укажите "yarn"
        .appName(f"VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}")
        .getOrCreate()
    )

    # напишите ваш код ниже


if __name__ == "__main__":
    main()