# @data_tests
# DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 4
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#c10c6123204441659a81321df8245b03

from pyspark.sql import SparkSession as _SparkSession

_spark = _SparkSession.builder.getOrCreate()

_book = [
    ("Harry Potter and the Goblet of Fire", "J. K. Rowling", 322),
    ("Nineteen Eighty-Four", "George Orwell", 382),
    ("Jane Eyre", "Charlotte Brontë", 159),
    ("Catch-22", "Joseph Heller", 174),
    ("The Catcher in the Rye", "J. D. Salinger", 168),
    ("The Wind in the Willows", "Kenneth Grahame", 259),
    ("The Mayor of Casterbridge", "Thomas Hardy", 300),
    ("Bad Girls", "Jacqueline Wilson", 299),
]

_columns = ["title", "author", "book_id"]

_df = _spark.createDataFrame(data=_book, schema=_columns)
_df_2 = _spark.createDataFrame(data=_book[2:], schema=_columns)

_test.fun_check('compare_df', {0: [_df], 1: [_df, _df_2]})
