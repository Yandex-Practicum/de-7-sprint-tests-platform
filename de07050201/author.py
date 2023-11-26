# напишите ваш код ниже
import datetime

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return [
        f"/user/USERNAME/data/events/date={(dt-datetime.timedelta(days=days)).strftime('%Y-%m-%d')}/event_type=message"
        for days in range(depth)
    ]