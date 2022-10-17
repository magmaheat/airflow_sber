import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from modules.pipeline import pipeline


path = os.path.expanduser('~/diploma_pro')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)


args = {
    'owner': 'diploma',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='ga_sessions',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start!"',
        dag=dag,
    )

    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )

    first_task >> pipeline

