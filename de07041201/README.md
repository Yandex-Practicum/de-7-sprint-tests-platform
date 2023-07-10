# Комментарий от тестописателя
Импорт `airflow` в тренажёре приведёт к ошибке доступа в каталог `/home/student/airflow`: 

`PermissionError: [Errno 13] Permission denied: '/home/student/airflow'`

Причина в том, что `airflow` пытается создать этот каталог, чтобы хранить в нём файлы конфигов и логов `airflow.cfg`, `webserver_config.py` и `logs`.

Но в контейнере для записи открыт только каталог `/home/student/tmp`.

Исправить можно через обновление переменной окружения `AIRFLOW_HOME` до импорта `airflow` — добавил их в прекод и авторское.

**Note:** в задачах с Airflow лучше отключать ворнинги чтобы избежать лишних вопросов от студентов. В JSON-конфиге контейнера это можно сделать через параметр `"precode"` — `"\nimport warnings\nwarnings.filterwarnings('ignore')\n"`

# Путь в админке для тестирования
DE (Аналитик 2.0 Beta) / Организация Data Lake / PySpark для инженера данных / Автоматизация джобы / Задание 1

# Ссылка на админку для тестирования
https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/9814e3a6-5fee-4a86-a7e2-cbbb1e650041/theory/

# Ссылка на ноушин с формулировкой задания
https://www.notion.so/praktikum/12-c0f9cd8ba271422cb868d4e0867aa2f3?pvs=4#a5ac9528666d4ef0af96bd218855ccb5