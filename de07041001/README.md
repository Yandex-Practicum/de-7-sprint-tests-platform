# Комментарий от тестописателя
На платформе невозможно реализовать Hadoop-кластер и HDFS — чтобы дать студенту возможность писать и проверять код на платформе, мы подготовили файловую структуру похожую на ту, что доступна на кластере.

Важные отличия:

1. Папки для чтения лежат в каталоге `/home/student` — если на кластере студент читает данные из `/user/master/data/events/date=2022-05-25`, то на платформе это будет `/home/student/user/master/data/events/date=2022-05-25`.

2. Папки для записи лежат в каталоге `/home/student/tmp` — если на кластере студент пишет данные в `/user/USERNAME/analytics/test`, то на платформе это будет `/home/student/tmp/user/USERNAME/analytics/test`.

3. На платформу не получится завести все данные с кластера. Взяли данные за `2022-05-01`, `2022-05-25` и `2022-05-31` в формате JSON из `master` и папки `channels`, `tags_verified` из `snapshots`. Создали каталог `USERNAME` с данными за май 2022-ого партиционированные по `date` и `event_type` в формате Parquet.

4. Имя пользователя на платформе `USERNAME`, а на кластере нужно будет использовать свой логин (выдаёт Telegram-бот).



# Путь в админке для тестирования
DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Собираем джобу / Задание 1

# Ссылка на админку для тестирования
https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/7fc52da1-720a-48b0-b40c-5fc485b8bdb2/theory/

# Ссылка на ноушин с формулировкой задания
https://www.notion.so/praktikum/10-e73390e02b8c4494af2a28fb2c68f88f?pvs=4#05fe4ecd2bfa4ee3a1dd5601e4ea7d66