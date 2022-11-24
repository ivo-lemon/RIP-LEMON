from airflow import DAG,settings
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from string import Template
from plugins import slack_util
from airflow_dbt_python.operators.dbt import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtSourceFreshnessOperator,
    DbtTestOperator,
    DbtDepsOperator
)
import json
import boto3
import os
from airflow.models.connection import Connection
session = settings.Session()  # type: ignore
existing = session.query(Connection).filter_by(conn_id="lemon").first()

if existing is None:
    # For illustration purposes, and to keep the example self-contained, we create
    # a Connection using Airflow's ORM. However, any method of loading connections would
    # work, like Airflow's UI, Airflow's CLI, or in deployment scripts.
    my_conn = Connection(
        conn_id="lemon",
        conn_type="redshift",
        description="A test redshift connection",
        schema="lemoncash_data" ,
        host="lemon-bi-data-2.crkgwab66zju.us-east-1.redshift.amazonaws.com",
        login="ivan_bergera",
        port=5439 ,
        password="xCV$H4k4I@6u2rg04aHb" , # pragma: allowlist secret
         #Other dbt parameters can be added as extras
       # extra=json.dumps(dict(threads=1,target="dev",dbname='lemon'))
    )

    session.add(my_conn)
    session.commit()

CHANNEL = "#airflow-monitors"
OWNERS= ["U02RTV264B1"]
DbtRunOperator.ui_color = "#ffdef2"


with DAG(
        dag_id="transactions_sot",
        start_date=days_ago(1), 
        schedule_interval='20 11 * * *',    
        #'*/10 * * * *',
        catchup=False,
        max_active_runs=1,
        tags=['SOT']) as dag:

    dbt_source_freshness= DbtSourceFreshnessOperator(
    task_id='dbt_source_freshness',
    target="lemon",
    project_dir='/opt/airflow/dags/transactions_sot/',
    #dbt_output='/opt/airflow/dags/transactions_sot/'
    )

    dbt_run = DbtRunOperator(
    task_id='dbt_run',
    target="lemon",
    #profile="transactions_sot",
    full_refresh=False,
    #dbt_bin='/home/airflow/.local/bin/dbt',
    profiles_dir=None,#'/opt/airflow/dags/fraud/',
    project_dir='/opt/airflow/dags/transactions_sot/'
    )


dbt_source_freshness >> dbt_run
    