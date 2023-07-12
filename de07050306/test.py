# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 2: портрет пользователя / Задание 6
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9ced9955-aae0-454d-b533-4505efb2770d/theory/
# https://www.notion.so/praktikum/3-2-38943ff76b604ef18826add9cf1c7c0a?pvs=4#4f53b333eb2543aab67273921685c9be

_test.call("DAG")

assert not _test.if_call("BashOperator"), "Для создания задач используйте только `SparkSubmitOperator`."

_test.call("SparkSubmitOperator")

_test.var_exists(
    "default_args",
    "dag_spark",
    "events_partitioned",
    "verified_tags_candidates_d7",
    "user_interests_d7",
    "user_interests_d28",
    same_type=True,
)

assert (
    _test.node(ast.Expr).node(left__id="events_partitioned", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__0__id="verified_tags_candidates_d7", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__1__id="verified_tags_candidates_d84", nclass=ast.BinOp).exists
), "Задачи `verified_tags_candidates_d7` и `verified_tags_candidates_d84`, должны зависеть от `events_partitioned`."

assert (
    _test.node(ast.Expr).node(left__id="events_partitioned", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__2__id="user_interests_d7", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__3__id="user_interests_d28", nclass=ast.BinOp).exists
), "Задачи `user_interests_d7` и `user_interests_d28`, должны зависеть от `events_partitioned`."
