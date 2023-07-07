import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .master("local")
    .appName("Learning DataFrames")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
# данные  датафрейма
data = [
    ("2021-01-06", 3744, 63, 322),
    ("2021-01-04", 2434, 21, 382),
    ("2021-01-04", 2434, 32, 159),
    ("2021-01-05", 3744, 32, 159),
    ("2021-01-06", 4342, 32, 159),
    ("2021-01-05", 4342, 12, 259),
    ("2021-01-06", 5677, 12, 259),
    ("2021-01-04", 5677, 23, 499),
]
# названия атрибутов
columns = ["dt", "user_id", "product_id", "purchase_amount"]
# создаём датафрейм
df = spark.createDataFrame(data=data, schema=columns)
# напишите ваш код ниже
window = Window().orderBy(df.purchase_amount)
df_window = df.withColumn("row_number", F.row_number().over(window))
df_window.select("dt", "user_id", "purchase_amount", "row_number").show()
