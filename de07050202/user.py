import datetime

from pyspark.sql import SparkSession


def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/home/student/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(depth)
    ]


spark = SparkSession.builder.master("local").getOrCreate()

# напишите ваш код ниже
