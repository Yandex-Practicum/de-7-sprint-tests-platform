from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


# напишите ваш код ниже



# не меняйте код ниже
df = spark.read.parquet("/home/student/user/USERNAME/data/events/date=2022-05-31")

result = compare_df(df, df)