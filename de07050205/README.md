# Комментарий от тестописателя
Импорт `airflow` в тренажёре приведёт к ошибке доступа в каталог `/home/student/airflow`: 

`PermissionError: [Errno 13] Permission denied: '/home/student/airflow'`

Причина в том, что `airflow` пытается создать этот каталог, чтобы хранить в нём файлы конфигов и логов `airflow.cfg`, `webserver_config.py` и `logs`.

Но в контейнере для записи открыт только каталог `/home/student/tmp`.

Исправить можно через обновление переменной окружения `AIRFLOW_HOME` до импорта `airflow` — добавил их в прекод и авторское.

**Note:** в задачах с Airflow лучше отключать ворнинги чтобы избежать лишних вопросов от студентов. В JSON-конфиге контейнера это можно сделать через параметр `"precode"` — `"\nimport warnings\nwarnings.filterwarnings('ignore')\n"`

# Путь в админке для тестирования
DE (Аналитик 2.0 Beta) / Организация Data Lake / Подготовка данных и наполнение Data Lake / Задача 1: обновление справочника тегов / Задание 5

# Ссылка на админку для тестирования
https://prestable.admin.praktikum.yandex-team.ru/faculties/d5b98ce5-3d91-47eb-ab9d-4df2bd9f465d/professions/4fdc51de-5615-472e-b31a-3c7ece22b3f0/tracks/88492e2d-9a7c-4123-a412-9aef5e024fe5/courses/f7eb0c28-7528-4c43-b1ce-f2b98b358282/topics/a7082746-1453-4b33-a129-96b3449f8de8/lessons/972d4605-1ae8-4126-a1ad-3104b58923f8/theory/

# Ссылка на ноушин с формулировкой задания
https://www.notion.so/praktikum/2-1-bc2eae94adfd4c61a17c953b1f288f5e?pvs=4#19f78088a06b419d88182c069fad14fa