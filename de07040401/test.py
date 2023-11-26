# @data_tests
# DE / Организация Data Lake / PySpark для инженера данных / Создание DataFrame и базовые операции / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/e86a4e6c-0f3f-431a-8fa5-6d539a6a9825/theory/
# https://www.notion.so/praktikum/4-DataFrame-bffcf9bdacba49479b2339013dff1749?pvs=4#101d031673204174ac507ad41ea2de79

assert not _test.if_call('stop'), 'Не используйте `stop` в вашем решении.'

_test.var_exists('spark', same_type=True, precode=True)

_test.call('createDataFrame')
_test.var_exists('df', same_type=True)

_u_df = _u.df.toPandas()
_a_df = _a.df.toPandas()

assert _u_df.equals(_a_df), (
    'Ваше значение `df` отличается от ожидаемого.\n'
    'Проверьте данные на которых создаёте датафрейм.'
)

_test.call('printSchema', obj=_u.df)

_test.output()
