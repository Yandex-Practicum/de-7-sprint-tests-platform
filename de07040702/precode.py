from pyspark.sql import SparkSession as _SparkSession
_spark = (
    _SparkSession
    .builder
    .config('spark.ui.showConsoleProgress', 'false')
    .config('spark.sql.shuffle.partitions', '1')
    .getOrCreate()
)
_spark.sparkContext.setLogLevel('OFF')