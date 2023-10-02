import sys

from pyspark.sql import SparkSession


def main():
    date = sys.argv[1] # '2022-05-31'
    days_count = sys.argv[2] # '5'
    suggested_cutoff = sys.argv[3] # '300'
    base_input_path = sys.argv[4] # '/home/student/tmp/user/USERNAME/data/events'
    verified_tags_path = sys.argv[5] # '/home/student/tmp/user/master/data/snapshots/tags_verified/actual'
    base_output_path = sys.argv[6] # '/home/student/tmp/user/USERNAME/data/analytics/verified_tags_candidates'

    spark = (
        SparkSession
        .builder
        .master("yarn")
        .appName(f"VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}")
        .getOrCreate()
    )

    # напишите ваш код ниже
    ...
    
    # напишите директорию записи в общем виде
    candidates.write...

if __name__ == "__main__":
    main()