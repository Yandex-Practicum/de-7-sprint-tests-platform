# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 3: портрет контактов пользователя / Задание 1
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc2f2b3-045c-4c23-b6bb-a1c85d302074/theory/
# https://www.notion.so/praktikum/4-3-cbbe0f4e30d74f0293bda436599a4ab6?pvs=4#4437f457b2514633bde5edfba95980b4

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
    "connection_interests_d7",
    same_type=True,
)

assert (
    _test.node(ast.Expr).node(right__id="connection_interests_d7", nclass=ast.BinOp).exists
), "Задача `connection_interests_d7`, должна зависеть от `events_partitioned` и `user_interests_d7`."