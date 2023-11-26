# # ВАЖНО: это код для запуска скрипта, в авторском его выполнять не нужно
# # Раскомментируйте, если будете передавать решение студенту

# import sys

# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# import datetime


# def main():
#     date = sys.argv[1]
#     days_count = sys.argv[2]
#     suggested_cutoff = sys.argv[3]
#     base_input_path = sys.argv[4]
#     verified_tags_path = sys.argv[5]
#     base_output_path = sys.argv[6]

#     spark = (
#         SparkSession
#         .builder
#         .master("yarn")
#         .appName(f"VerifiedTagsCandidatesJob-{date}-d{days_count}-cut{suggested_cutoff}")
#         .getOrCreate()
#     )

#     # напишите ваш код ниже
#     dt = datetime.datetime.strptime(date, "%Y-%m-%d")
#     paths = [
#         f"{base_input_path}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
#         for x in range(int(days_count))
#     ]
#     messages = spark.read.parquet(*paths)

#     verified_tags = spark.read.parquet(verified_tags_path)

#     candidates = find_candidates(messages, verified_tags, suggested_cutoff)

#     # напишите директорию записи в общем виде
#     candidates.write.parquet(f"{base_output_path}/date={date}")


# def find_candidates(messages, verified_tags, suggested_cutoff):
#     all_tags = (
#         messages.where("event.message_channel_to is not null")
#         .selectExpr(["event.message_from as user", "explode(event.tags) as tag"])
#         .groupBy("tag")
#         .agg(F.countDistinct("user").alias("suggested_count"))
#         .where(F.col("suggested_count") >= suggested_cutoff)
#     )

#     return all_tags.join(verified_tags, "tag", "left_anti")


# if __name__ == "__main__":
#     main()