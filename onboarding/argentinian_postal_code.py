from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import requests
import json
from plugins.s3_utils import read_s3_csv_to_pandas, s3_folder_exists
from ratelimiter import RateLimiter
import math


EXECUTION_DATE = datetime.utcnow()
EXECUTION_DATE_YEAR = str(EXECUTION_DATE.year).zfill(4)
EXECUTION_DATE_MONTH = str(EXECUTION_DATE.month).zfill(2)
EXECUTION_DATE_DAY = str(EXECUTION_DATE.day).zfill(2)
EXECUTION_DATE_HOUR = str(EXECUTION_DATE.hour).zfill(2)

# kore urls
KORE_HOST = "https://api.lemoncash.com.ar/api/v1"
UPDATE_POSTAL_CODE_ENDPOINT = f"{KORE_HOST}/priv/accounts/bulk/argentinian-postal-code"
rate_limiter = RateLimiter(max_calls=1, period=3)
BATCH_SIZE = 1000
SERVICE_TOKEN = Variable.get("airflow_service_token")

# s3 urls
S3_GENERAL_BUCKET = "data-airflow-tables-queries-production"
S3_QUERY_PATH = "query/argentinian_postal_code/"
S3_TABLE_PATH = "tables/argentinian_postal_code/"
S3_DATE_PATH = f"year={EXECUTION_DATE_YEAR}/month={EXECUTION_DATE_MONTH}/day={EXECUTION_DATE_DAY}/hour={EXECUTION_DATE_HOUR}"

ARG_POSTAL_CODES_FILENAME = "all_arg_postal_codes.parquet"
RESULTS_CSV_PATH = f"{S3_TABLE_PATH}{S3_DATE_PATH}/results"

ECS_SUBNET = Variable.get("ECS_SUBNET")
ECS_SECURITY_GROUP = Variable.get("ECS_SECURITY_GROUP")

# query for fetching accounts
QUERY = f"""
SELECT * FROM (
	    SELECT 
		-- execution date
		'{EXECUTION_DATE_YEAR}' as year,
		'{EXECUTION_DATE_MONTH}' as month,
		'{EXECUTION_DATE_DAY}' as day,
		'{EXECUTION_DATE_HOUR}' as hour,
		
		A.id, A.street, A.number, A.locality, A.province, A.zip_code 
		
		FROM lemoncash_ar.Accounts A 
		INNER JOIN lemoncash_ar.Users U ON U.id = A.owner_id 
		WHERE U.operation_country = 'ARG'
		AND (A.argentinian_postal_code = 'X' OR A.argentinian_postal_code is null)
		AND (A.street is not null or number is not null or A.locality is not null or A.province is not null or A.zip_code is not null)
		ORDER BY RAND()
"""

QUERY_ESCAPED = QUERY.replace("'", "\\'")

UNLOAD_ACCOUNTS_QUERY = "unload ('" + QUERY_ESCAPED + \
                        "LIMIT {{ params.query_limit }})')  " \
                        "to 's3://" + S3_GENERAL_BUCKET + "/" + S3_TABLE_PATH + \
                        "' iam_role 'arn:aws:iam::127071149305:role/RedshiftS3' " \
                        "parquet partition by (year, month, day, hour) allowoverwrite;"


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))


def update_postal_code():
    results_parquet_file = f"s3://{S3_GENERAL_BUCKET}/{RESULTS_CSV_PATH}"
    if not s3_folder_exists(results_parquet_file):
        print(f"skipping because of empty folder: {results_parquet_file}")
        return

    df = read_s3_csv_to_pandas(S3_GENERAL_BUCKET, RESULTS_CSV_PATH + '/results.csv')

    data = []
    for _, row in df.iterrows():
        data.append({'account_id': row['id'], 'argentinian_postal_code': row['final_cpa']})

    print(f'Updating {len(data)} rows')
    batches = list(chunker_list(data, math.ceil(len(data)/BATCH_SIZE)))
    for batch in batches:
        with rate_limiter:
            do_update_postal_code(batch)

    return


def do_update_postal_code(body):
    url = UPDATE_POSTAL_CODE_ENDPOINT
    headers = {
        'Content-type': 'application/json',
        'Authorization': f'{SERVICE_TOKEN}'
    }
    r = requests.patch(url, data=json.dumps(body), headers=headers)
    if r.status_code != 200:
        msg = f"got a status code: {r.status_code}"
        raise Exception(msg)

with DAG(
        dag_id="argentinian_postal_code",
        start_date=datetime(2022, 8, 10),
        schedule_interval='0 * * * *',
        tags=['onboarding', 'postalcode'],
        catchup=False,
) as dag:
    query_limit = Variable.get("query_limit", default_var=10)

    task_compute_pending_accounts = RedshiftSQLOperator(
        task_id='compute_pending_accounts',
        sql=UNLOAD_ACCOUNTS_QUERY,
        params={'query_limit': query_limit},
        dag=dag,
    )

    task_calculate_postal_code = ECSOperator(
        task_id='calculate_postal_code',
        cluster='lemon-ecs-cluster',
        task_definition='cpa-calculator',
        region_name='us-east-1',
        launch_type='FARGATE',
        overrides={
            'containerOverrides': [
                {
                    'name': 'cpa-calculator',
                    'command': [S3_DATE_PATH],
                },
            ],
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': [ECS_SUBNET],
                'securityGroups': [ECS_SECURITY_GROUP],
            }
        },
    )

    task_update_postal_code = PythonOperator(
        task_id='update_postal_code',
        python_callable=update_postal_code,
        dag=dag,
    )

    task_compute_pending_accounts >> task_calculate_postal_code >> task_update_postal_code

