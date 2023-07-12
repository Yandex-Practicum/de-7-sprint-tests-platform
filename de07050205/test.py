# @data_tests
# DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 5
# https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/
# https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#19f78088a06b419d88182c069fad14fa

_test.call("DAG")

assert not _test.if_call("BashOperator"), "Для создания задач используйте только `SparkSubmitOperator`."

_test.call("SparkSubmitOperator")

_test.var_exists(
    "default_args",
    "dag_spark",
    "events_partitioned",
    "verified_tags_candidates_d7",
    "verified_tags_candidates_d84",
    same_type=True,
)

assert (
    _test.node(ast.Expr).node(left__id="events_partitioned", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__0__id="verified_tags_candidates_d7", nclass=ast.BinOp).exists
    and _test.node(ast.Expr).node(right__elts__1__id="verified_tags_candidates_d84", nclass=ast.BinOp).exists
), "Задачи `verified_tags_candidates_d7` и `verified_tags_candidates_d84`, должны зависеть от `events_partitioned`."