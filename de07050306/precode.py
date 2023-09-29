from warnings import filterwarnings as _filterwarnings
_filterwarnings('ignore')
from os import environ as _environ
_environ['AIRFLOW_HOME'] = '/home/student/tmp'