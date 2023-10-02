import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("yarn").getOrCreate()

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/home/student/tmp/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=days)).strftime('%Y-%m-%d')}/event_type=message"
        for days in range(depth)
    ]


paths = input_paths("2022-05-31", 7)

# напишите ваш код ниже
