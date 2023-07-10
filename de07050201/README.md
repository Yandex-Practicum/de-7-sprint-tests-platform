# Комментарий от тестописателя
На платформе невозможно реализовать Hadoop-кластер и HDFS — чтобы дать студенту возможность писать и проверять код на платформе, мы подготовили файловую структуру похожую на ту, что доступна на кластере.

Важные отличия:

1. Папки для чтения лежат в каталоге `/home/student` — если на кластере студент читает данные из `/user/master/data/events/date=2022-05-25`, то на платформе это будет `/home/student/user/master/data/events/date=2022-05-25`.

2. Папки для записи лежат в каталоге `/home/student/tmp` — если на кластере студент пишет данные в `/user/USERNAME/analytics/test`, то на платформе это будет `/home/student/tmp/user/USERNAME/analytics/test`.

3. На платформу не получится завести все данные с кластера. Взяли данные за `2022-05-01`, `2022-05-25` и `2022-05-31` в формате JSON из `master` и папки `channels`, `tags_verified` из `snapshots`. Создали каталог `USERNAME` с данными за май 2022-ого партиционированные по `date` и `event_type` в формате Parquet.

4. Имя пользователя на платформе `USERNAME`, а на кластере нужно будет использовать свой логин (выдаёт Telegram-бот).

# Путь в админке для тестирования
DE / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 1

# Ссылка на админку для тестирования
https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/48679252-cc63-48c4-a6bc-22c2d65a35e3/tracks/fe2a2f3d-d6d9-4e76-a04d-69763bb6fee2/courses/8699ef32-4cf5-40d5-ba66-d63d7fc172d9/topics/69e4b265-1ad5-4db0-b996-a5c3fe1f54b4/lessons/1867df6d-bf90-484e-bbc6-f9521fbe0ae2/theory/

# Ссылка на ноушин с формулировкой задания
https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#78645ce8fa1f4594945019abd194258c