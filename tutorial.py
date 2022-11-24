# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import timedelta,datetime
from textwrap import dedent

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

with DAG(
    'tutorial',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='t1',
        bash_command='pwd ',
    )
    t2 = BashOperator(
        task_id='t2',
        bash_command='ls /opt/airflow/dags/ ',
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command='ls /opt/airflow/dags/transactions_sot/ ',
    )

    t1 >>t2 >> t3

# para saber en que path relativo corre
 # para saber si es que efectivamente en prod usa este path
 # para saber si es que falta algo aca