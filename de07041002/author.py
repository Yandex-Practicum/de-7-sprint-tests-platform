# ВАЖНО: это код для запуска скрипта, в авторском его выполнять не нужно
# Раскомментируйте, если будете передавать решение студенту

# import sys

# from pyspark.sql import SparkSession


# def main():
#     date = sys.argv[1]
#     base_input_path = sys.argv[2]
#     base_output_path = sys.argv[3]

#     spark = (
#         SparkSession.builder
#         .master("local") # для запуска на кластере укажите "yarn"
#         .appName(f"EventsPartitioningJob-{date}")
#         .getOrCreate()
#     )

#     # напишите директорию чтения в общем виде
#     events = spark.read.json(f"{base_input_path}/date={date}")

#     # напишите директорию записи в общем виде
#     (
#         events.write
#         .partitionBy("event_type")
#         .mode("overwrite")
#         .format("parquet")
#         .save(f"{base_output_path}/date={date}")
#     )


# if __name__ == "__main__":
#     main()