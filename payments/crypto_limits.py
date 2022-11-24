from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from plugins import slack_util

from datetime import datetime
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
EXECUTION_DATE = datetime.utcnow().date()
EXECUTION_DATE_YEAR = str(EXECUTION_DATE.year).zfill(4)
EXECUTION_DATE_MONTH = str(EXECUTION_DATE.month).zfill(2)
EXECUTION_DATE_DAY = str(EXECUTION_DATE.day).zfill(2)
FIRST_DAY_OF_CURRENT_MONTH = EXECUTION_DATE.replace(day=1)
FIRST_DAY_OF_CURRENT_YEAR = EXECUTION_DATE.replace(month=1, day=1)

FIAT_CURRENCY = 'MONEY'

TABLE_S3_PATH = "s3://data-airflow-tables-queries-production/tables/crypto_limits_enable_autoswap"

KORE_HOST = "https://api.lemoncash.com.ar/api/v1"
ENABLE_AUTOSWAP_PATH = f"{KORE_HOST}/priv/cards/autoswap/enablement"
SERVICE_TOKEN = Variable.get("airflow_service_token")

BUCKET_URI = f'{TABLE_S3_PATH}/year={EXECUTION_DATE_YEAR}/month={EXECUTION_DATE_MONTH}/day={EXECUTION_DATE_DAY}'

QUERY = f"""
with disabled_accounts as (

	select
		c.id as account_id,
		a.updated_at as card_account_updated_at,
		cast(d.monthly_crypto_sale_limit_amount as decimal(36,8)),
		cast(d.yearly_crypto_sale_limit_amount as decimal(36,8))

	from lemoncash_ar.cardaccounts a  
	inner join lemoncash_ar.accounts c on a.user_account_id = c.id 
	inner join lemoncash_ar.levels d on c.level_id = d.id 
	where a.autoswap_enabled = 0
	
), monthly_crypto_sales as (

	select
		sum(income_amount) as monthly_sales,
		account_id,
		date_trunc('month',created_at) as ts
	from lemoncash_ar.cryptosaletransactions b
	where ts >= '{ FIRST_DAY_OF_CURRENT_MONTH }'
	and destination_currency = '{ FIAT_CURRENCY }'
	group by account_id, ts
	
), yearly_crypto_sales as (
	
	select
		sum(income_amount) as yearly_sales,
		account_id,
		date_trunc('year',created_at) as ts
	from lemoncash_ar.cryptosaletransactions b
	where ts >= '{ FIRST_DAY_OF_CURRENT_YEAR }'
	and destination_currency = '{ FIAT_CURRENCY }'
	group by account_id, ts

), locked_autoswap_sales as (
	
		select
			l.account_id,
			sum(l.requested_amount) as locked_monthly
		from lemoncash_ar.lemoncardpaymentcontracts l
		join lemoncash_ar.lemoncardpaymentcontractterms l2 
		on l.id = l2.lemon_card_payment_contract_id 
		where true
		and l.created_at >= '{ FIRST_DAY_OF_CURRENT_MONTH }'
		and l.state = 'LOCKED'
		and l.requested_currency = '{ FIAT_CURRENCY }'
		and l2.spending_currency != '{ FIAT_CURRENCY }' --autoswap enabled
		group by l.account_id
		
), joined as (

	select
		a.*,
		coalesce(m.monthly_sales, 0) as monthly_sales,
		coalesce(y.yearly_sales, 0) as yearly_sales,
		coalesce(l.locked_monthly, 0) as locked_monthly_sales
	from disabled_accounts a  
	left join monthly_crypto_sales m on a.account_id = m.account_id
	left join yearly_crypto_sales y on a.account_id = y.account_id
	left join locked_autoswap_sales l on a.account_id = l.account_id
	
), users_to_enable as (

	select
		-- execution date
		'{EXECUTION_DATE_YEAR}' as year,
		'{EXECUTION_DATE_MONTH}' as month,
		'{EXECUTION_DATE_DAY}' as day,

		*
	from joined
	where true
	and (locked_monthly_sales + monthly_sales) < monthly_crypto_sale_limit_amount
	and (locked_monthly_sales + yearly_sales) < yearly_crypto_sale_limit_amount
)

select *
from users_to_enable
"""

QUERY_ESCAPED = QUERY.replace("'", "\\'")

QUERY_WITH_WRITE = f"""
unload ('
{ QUERY_ESCAPED }
')
to '{ TABLE_S3_PATH }'
iam_role 'arn:aws:iam::127071149305:role/RedshiftS3'
parquet
partition by (year, month, day)
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

def enable_account(acc_id: str):
	print(acc_id)
	
	headers = {
		'Content-type': 'application/json',
		'Authorization': f'{SERVICE_TOKEN}'
	}
	data = {
		'account_id': acc_id
	}
	r = requests.post(ENABLE_AUTOSWAP_PATH, data=json.dumps(data), headers=headers)
	if r.status_code != 201:
		# time to panic
		# TODO(sanchez): should we wrap this into a try catch and persist the failed accounts?
		msg = f"got a status code: { r.status_code }"
		raise Exception(msg)


def update_affected_users():
	if not s3_folder_exists(BUCKET_URI):
		# previous step didn't throw any row
		return
	df = read_s3_parquet_to_pandas(BUCKET_URI)
	for _, row in df.iterrows():
		enable_account(row['account_id'])

	return

with DAG(
	dag_id="crypto_limits_enable_autoswap",
	start_date=datetime(2022, 7, 4),
	schedule_interval='0 11 * * *', # 11am UTC = 8am GMT-3
	tags=['payments', 'autoswap', 'limits'],
	catchup=False,
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
	) as dag:
		task_compute_affected_users = RedshiftSQLOperator(
			task_id='compute_affected_users',
			sql=QUERY_WITH_WRITE,
		)

		task_update_affected_users = PythonOperator(
			task_id='update_affected_users',
			python_callable= update_affected_users,
			dag=dag,
		)

		task_compute_affected_users >> task_update_affected_users
