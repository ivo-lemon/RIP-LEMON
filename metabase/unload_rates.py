from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta,datetime
import os

with DAG(
    dag_id="unload_rates", 
    start_date=datetime(2022, 7, 22, 23, 57), 
    schedule_interval='*/1 * * * *', 
    tags=['exchangerates','minute']) as dag:    

    unload_rates = RedshiftSQLOperator(
        task_id='unload_rates', 
        sql=""" unload ('select id, 
        base_currency as asset,
        quote_currency,
        buy_price,
        sell_price,
        created_at,
        updated_at,
        refreshable,
         extract(minute from getdate()) as minute,
         extract(hour from getdate()) as hour,
         extract(day from getdate()) as day,
         extract(month from getdate()) as month,
         extract(year from getdate()) as year
from lemoncash_ar.exchangeratesv2
where quote_currency = ''USD'' or quote_currency = ''MONEY'' or quote_currency = ''BRL''')
to 's3://data-exchange-rates-virginia/rates/'
iam_role 'arn:aws:iam::127071149305:role/RedshiftS3'
parquet
partition by (year, month, day, hour, minute,asset)
allowoverwrite""")

    
    unload_rates 

