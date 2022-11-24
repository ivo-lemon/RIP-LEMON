from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from plugins import slack_util

from datetime import datetime, timedelta
import s3fs
import pyarrow.parquet as pq
import requests
import json
import boto3
import json
from urllib.parse import urlparse

CHANNEL = "#airflow-monitors"
OWNERS = [
    "U03BT8QGFEX" #@AgustinSanchez
]

# TODO(sanchez): use airflow macro ds instead? >> We can't use it outside rendering
EXECUTION_DATE = datetime.utcnow()
EXECUTION_DATE_YEAR = str(EXECUTION_DATE.year).zfill(4)
EXECUTION_DATE_MONTH = str(EXECUTION_DATE.month).zfill(2)
EXECUTION_DATE_DAY = str(EXECUTION_DATE.day).zfill(2)
EXECUTION_DATE_HOUR = str(EXECUTION_DATE.hour).zfill(2)

MIN_CONTRACT_DATE = EXECUTION_DATE.date() - timedelta(3) # not rolling window
LOCKED_OFFSET_MINUTES = 1

TABLE_S3_PATH = "s3://data-airflow-tables-queries-production/tables/dlq_retry_contracts"

KORE_HOST = "https://api.lemoncash.com.ar/api/v1"
EXECUTE_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT = f"{KORE_HOST}/lemon-card-payment-contracts/{{contract_id}}/completion"
ROLLBACK_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT = f"{KORE_HOST}/lemon-card-payment-contracts/{{contract_id}}/execute-rollback"
CANCEL_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT = f"{KORE_HOST}/lemon-card-payment-contracts/{{contract_id}}/execute-cancellation"

SERVICE_TOKEN = Variable.get("airflow_service_token")
LEMON_CARD_PAYMENT_TRANSACTION_CANCELLER_TOKEN = Variable.get("LEMON_CARD_PAYMENT_TRANSACTION_CANCELLER_TOKEN")

BUCKET_URI = f'{TABLE_S3_PATH}/year={EXECUTION_DATE_YEAR}/month={EXECUTION_DATE_MONTH}/day={EXECUTION_DATE_DAY}/hour={EXECUTION_DATE_HOUR}'

QUERY = f"""
select
	-- execution date
	'{EXECUTION_DATE_YEAR}' as year,
	'{EXECUTION_DATE_MONTH}' as month,
	'{EXECUTION_DATE_DAY}' as day,
	'{EXECUTION_DATE_HOUR}' as hour,

	id as contract_id,
	authorization_request_id,
	account_id,
	state,
	created_at,
	updated_at,
	datediff(minute, created_at, getdate()) as locked_by_minutes
FROM lemoncash_ar.LemonCardPaymentContracts
where state in (
	'FILLED',
	'PENDING_ROLLBACK',
	'PENDING_CANCELLATION'
)
and (created_at >= '{MIN_CONTRACT_DATE}' or updated_at >= '{MIN_CONTRACT_DATE}')
-- ignore too recent contracts
and locked_by_minutes >= {LOCKED_OFFSET_MINUTES}
"""

QUERY_ESCAPED = QUERY.replace("'", "\\'")

QUERY_WITH_WRITE = f"""
unload ('
{ QUERY_ESCAPED }
')
to '{ TABLE_S3_PATH }'
iam_role 'arn:aws:iam::127071149305:role/RedshiftS3'
parquet
partition by (year, month, day, hour)
allowoverwrite;
"""

def get_bucket_and_prefix_from_s3_uri(uri: str):
	o = urlparse(uri, allow_fragments=False)
	return (o.netloc, o.path.lstrip('/'))


def s3_folder_exists(uri: str) -> bool:
	bucket, path = get_bucket_and_prefix_from_s3_uri(uri)
	s3 = boto3.client('s3')
	path = path.rstrip('/') 
	resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/',MaxKeys=1)
	return 'CommonPrefixes' in resp

def read_s3_parquet_to_pandas(bucket_uri):
	fs = s3fs.S3FileSystem()
	dataset = pq.ParquetDataset(bucket_uri, filesystem=fs)
	df = dataset.read().to_pandas()
	return df


def retry_filled_contracts():
	if not s3_folder_exists(BUCKET_URI):
		print(f"skipping because of empty folder: {BUCKET_URI}")
		return
	df = read_s3_parquet_to_pandas(BUCKET_URI)
	df = df.query("state == 'FILLED'")
	for _, row in df.iterrows():
		execute_filled_contract(row['contract_id'])

	return

def execute_filled_contract(contract_id: str):
	print(contract_id)

	url = EXECUTE_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT.replace("{contract_id}", contract_id)
	headers = {
		'Content-type': 'application/json',
		'Authorization': f'{SERVICE_TOKEN}'
	}
	data = {}
	r = requests.post(url, data=json.dumps(data), headers=headers)

	if r.status_code != 201:
		# time to panic
		# TODO(sanchez): should we wrap this into a try catch and persist the failed cases?
		msg = f"got a status code: { r.status_code }"
		raise Exception(msg)


def retry_pending_rollback_contracts():
	if not s3_folder_exists(BUCKET_URI):
		print(f"skipping because of empty folder: {BUCKET_URI}")
		return
	df = read_s3_parquet_to_pandas(BUCKET_URI)
	df = df.query("state == 'PENDING_ROLLBACK'")
	for _, row in df.iterrows():
		rollback_contract(row['contract_id'])

	return

def rollback_contract(contract_id: str):
	print(contract_id)

	url = ROLLBACK_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT.replace("{contract_id}", contract_id)
	headers = {
		'Content-type': 'application/json',
		'Authorization': f'{LEMON_CARD_PAYMENT_TRANSACTION_CANCELLER_TOKEN}'
	}
	data = {}
	r = requests.post(url, data=json.dumps(data), headers=headers)

	if r.status_code != 201:
		# time to panic
		# TODO(sanchez): should we wrap this into a try catch and persist the failed cases?
		msg = f"got a status code: { r.status_code }"
		raise Exception(msg)


def retry_pending_cancellation_contracts():
	if not s3_folder_exists(BUCKET_URI):
		print(f"skipping because of empty folder: {BUCKET_URI}")
		return
	df = read_s3_parquet_to_pandas(BUCKET_URI)
	df = df.query("state == 'PENDING_CANCELLATION'")
	for _, row in df.iterrows():
		cancel_contract(row['contract_id'])

	return

def cancel_contract(contract_id: str):
	print(contract_id)

	url = CANCEL_LEMON_CARD_PAYMENT_CONTRACT_ENDPOINT.replace("{contract_id}", contract_id)
	headers = {
		'Content-type': 'application/json',
		'Authorization': f'{LEMON_CARD_PAYMENT_TRANSACTION_CANCELLER_TOKEN}'
	}
	data = {}
	r = requests.post(url, data=json.dumps(data), headers=headers)

	if r.status_code != 200:
		# time to panic
		# TODO(sanchez): should we wrap this into a try catch and persist the failed cases?
		msg = f"got a status code: { r.status_code }"
		raise Exception(msg)

with DAG(
	dag_id="dlq_retry_contracts",
	start_date=datetime(2022, 7, 4),
	schedule_interval=None,
	tags=['payments', 'contracts', 'dlq'],
	catchup=False,
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
	) as dag:
		task_compute_pending_contracts = RedshiftSQLOperator(
			task_id='compute_pending_contracts',
			sql=QUERY_WITH_WRITE,
		)

		task_retry_filled_contracts = PythonOperator(
			task_id='retry_filled_contracts',
			python_callable= retry_filled_contracts,
			dag=dag,
		)

		task_retry_pending_rollback_contracts = PythonOperator(
			task_id='retry_pending_rollback_contracts',
			python_callable= retry_pending_rollback_contracts,
			dag=dag,
		)

		task_retry_pending_cancellation_contracts = PythonOperator(
			task_id='retry_pending_cancellation_contracts',
			python_callable= retry_pending_cancellation_contracts,
			dag=dag,
		)

		task_compute_pending_contracts >> task_retry_filled_contracts
		task_compute_pending_contracts >> task_retry_pending_rollback_contracts
		task_compute_pending_contracts >> task_retry_pending_cancellation_contracts
