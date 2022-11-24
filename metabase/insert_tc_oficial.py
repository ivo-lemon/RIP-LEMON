from distutils import text_file
from numpy import insert
import pandas as pd 
import os 
import requests
from decimal import Decimal
import os  
from pathlib import Path
from datetime import timedelta
from datetime import timedelta,datetime, date
import pandas as pd 
import boto3
from io import StringIO 
import json 
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from plugins import slack_util
CHANNEL = "#airflow-monitors"
OWNERS= ["U035P7MR3UZ"]

#me devuelve una linea con la info de ayer --> [dia, tc]
def get_tc():
    url = 'https://api.estadisticasbcra.com/usd_of'
    header = {'Authorization': 'BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2OTc2MzYzMTcsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJtYXRpdHV0aW5AaG90bWFpbC5jb20ifQ.FLHwTtjVLuTTmEcDTqpV44b4u0sRpXWY9b23PBLB_OKgX0Ggzy1KSEfbCvEAVhd-IyhLJrvxwdBASyLKL6rRug'}
    r = requests.get(url, headers=header)
    j = r.json()
    tc = j[len(j)-1]
    dia = datetime.strptime(tc['d'], '%Y-%m-%d').date()
    cot = round(Decimal(tc['v']),2)
    df = pd.DataFrame({'Dia': dia, 'TC': cot}, index=[1])
    return df

def upload_s3 (file):
    # CREO EL NOMBRE DEL FILE 
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    date = yesterday.strftime("%Y-%m-%d")
    filename = 'tc_oficial/csv/'+date+'/file_'+date+'.csv'
    print(f"df_{date}")

    # LO TIRO EN S3
    bucket = 'lemoncash-data-cvu' 
    csv_buffer = StringIO()
    file.to_csv(csv_buffer)
    dt = datetime.now()
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    print(file)
    print('subido a s3')

def upload_exchange_rate():
    info = get_tc()
    upload_s3(info)

today = datetime.now()
yesterday = today - timedelta(days=1)
date = yesterday.strftime("%Y-%m-%d")

def check_date():
    a = get_tc()
    b = a._get_value(1,'Dia')
    if b == yesterday.date():
        return 'go_on'
    else:
        return 'change_date_to_yesterday'

query = f"""    
    copy bi_tools.staging_tc
    from 's3://lemoncash-data-cvu/tc_oficial/csv/{date}/file_{date}.csv'
    iam_role 'arn:aws:iam::127071149305:role/RedshiftS3'
    CSV
    region 'sa-east-1'
    IGNOREHEADER 1 
    ;
    """

with DAG(
    dag_id="insert_tc_oficial", 
    start_date=datetime(2022, 10, 20), 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    schedule_interval= '30 8 * * *',
    tags = ['tc_oficial','table','incremental','daily']
    ) as dag:

    insert_in_s3 = PythonOperator(
        task_id = 'get_tc_and_insert_in_s3_bucket',
        python_callable = upload_exchange_rate
    )

    copy_from_bucket = RedshiftSQLOperator(
        task_id = 'copy_from_s3_bucket',
        sql = query
    )

    create_staging_table = RedshiftSQLOperator(
       task_id='create_staging_table', 
       sql=""" CREATE TABLE bi_tools.STAGING_tc(
           "index" int,
           "dia" date,
           "tc" decimal(36,8)
           );  """)

    insert_prod = RedshiftSQLOperator(
       task_id = 'insert_tc_in_prod',
       sql = """ insert into bi_tools.tc_oficial (select dia, tc from bi_tools.staging_tc) """
    )

    check_wrong_dates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_wrong_dates",
    sql="""WITH stage_table as (select 1 as test,max(dia) as max_date from bi_tools.staging_tc),
       prod_table as (select 1 as test, max(dia) as max_date from bi_tools.tc_oficial)
       select stage_table.test,
       case when stage_table.max_date <= prod_table.max_date then 0 else 1 end as date_check
       from stage_table inner join prod_table on stage_table.test = prod_table.test
    """
    )

    check_duplicates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_duplicates",
    sql="""WITH null_rows as (
       select count(*) as duplicated_events from (
       select dia, tc, COUNT(*)
       from bi_tools.staging_tc
       group by 1,2
       HAVING COUNT(*) > 1))
       select case when duplicated_events > 0 then 0 else 1 end as duplicated_check
       from null_rows
    """
    )

    check_nulls = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_nulls",
    sql="""
        WITH null_rows as (
            select count(*) as null_events from (
                select *
                from bi_tools.staging_tc
                where dia is null or tc is null
                )
            )
        select case when null_events > 0 then 0 else 1 end as null_check
        from null_rows
    """
    )

    check_weekend = BranchPythonOperator(
        task_id = 'check_weekend',
        python_callable = check_date
    )

    go_on = DummyOperator(task_id='go_on', dag=dag)

    continue_on = DummyOperator(task_id='continue_task_again',
            trigger_rule='none_failed_or_skipped',
            dag=dag
    )

    change_date_to_yesterday = RedshiftSQLOperator(
        task_id = 'change_date_to_yesterday',
        sql = """ UPDATE bi_tools.staging_tc
                SET Dia = (current_date - '1 Day'::interval); """
    )

    delete_staging = RedshiftSQLOperator(
        task_id="delete_staging", 
        sql= """ DROP TABLE bi_tools.staging_tc
            """,
        trigger_rule="all_done"
    )
    
    create_staging_table>>insert_in_s3>>copy_from_bucket>>check_weekend>>[go_on,change_date_to_yesterday]>>continue_on>>[check_wrong_dates,check_duplicates,check_nulls]>>insert_prod>>delete_staging
    
    
    