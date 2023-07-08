# напишите ваш код ниже
import datetime

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/home/student/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message"
        for x in range(depth)
    ]

# код ниже не меняйте
paths = input_paths("2022-05-31", 7)
print(paths)