from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local")
    # .config("spark.sql.shuffle.partitions", "1")
    # .config('spark.executor.instances', '1')
    # .config('spark.dynamicAllocation.enabled', 'false')
    .getOrCreate()
)


# напишите ваш код ниже
