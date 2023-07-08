# Комментарий от тестописателя
Импорт `airflow` в тренажёре приведёт к ошибке доступа в каталог `/home/student/airflow`: 

`PermissionError: [Errno 13] Permission denied: '/home/student/airflow'`

Причина в том, что `airflow` пытается создать этот каталог, чтобы хранить в нём файлы конфигов и логов `airflow.cfg`, `webserver_config.py` и `logs`.

Но в контейнере для записи открыт только каталог `/home/student/tmp`.

Исправить можно через обновление переменной окружения `AIRFLOW_HOME` до импорта `airflow` — добавил их в прекод и авторское.

**Note:** в задачах с Airflow лучше отключать ворнинги чтобы избежать лишних вопросов от студентов. В JSON-конфиге контейнера это можно сделать через параметр `"precode"` — `"\nimport warnings\nwarnings.filterwarnings('ignore')\n"`