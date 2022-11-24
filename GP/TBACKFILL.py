from airflow import DAG, settings, secrets
from airflow.operators.sql import SQLCheckOperator,SQLValueCheckOperator, SQLIntervalCheckOperator,SQLThresholdCheckOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import boto3
import json
from plugins import slack_util

PythonOperator.ui_color = "#ffdef2"
RedshiftSQLOperator.ui_color = "#ddfffc"
S3ToRedshiftOperator.ui_color = "#f2e2ff"
SQLCheckOperator.ui_color ="#ffffe3"
CHANNEL = "#airflow-monitors"
OWNERS= ["U02RTV264B1"]

def call_lambda_2001():
    hook = AwsLambdaHook(function_name = 'gp_2001',
                         region_name='sa-east-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse')
    response_1 = hook.invoke_lambda(payload=json.dumps({ "RUN": "BACKFILL" }))
    print(response_1)

with DAG(
    dag_id="GP_BACKFILL", 
    start_date= days_ago(1), 
    schedule_interval='50 02 * * *',  
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['GP']) as dag:

    invoke_lambda_function = PythonOperator(
        task_id='invoke_lambda_function',
        python_callable = call_lambda_2001,
    )


invoke_lambda_function
