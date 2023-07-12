from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


# напишите ваш код ниже
def compare_df(left, right):
    return (set(left.columns) == set(right.columns)) & (
        left.count()
        == right.count()
        == left.unionByName(right).distinct().count()
    )


# не меняйте код ниже
df = spark.read.parquet("/home/student/user/USERNAME/data/events/date=2022-05-31")

result = compare_df(df, df)
