# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins import slack_util
from datetime import datetime

CHANNEL = "#airflow-monitors"
OWNERS = [
    "U03BT8QGFEX" #@AgustinSanchez
]

def dummy_step():
    raise Exception("Time to Panic")


with DAG(
    dag_id = 'example_dag',
    on_success_callback=None,
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    description='Lorem',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    t1 = PythonOperator(
        task_id='task_name_t1',
        python_callable=dummy_step,
    )

    t1
