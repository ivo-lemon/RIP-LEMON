from airflow import DAG
from datetime import timedelta,datetime
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

with DAG(
	dag_id="balances_snapshot",
	start_date=datetime(2022, 6, 24),
	schedule_interval='@hourly',
	tags=['wallets', 'balances']) as dag:
		task_get_all_balances = RedshiftSQLOperator(
			task_id='task_get_all_balances', sql="""unload ('with balances as (
	select
		a.userid user_id,
		a.assettypeid asset,
		cast(a.balance as decimal(36,8)) amount,
		cast(a.balance as decimal(36,8))*cast(b.buy_price as decimal(36,8)) as usd_amount,
		cast(b.buy_price as decimal(36,8)) rate
	from
		lemoncash_ar.wallets a
	inner join
		lemoncash_ar.exchangeratesv2 b on a.assettypeid = b.base_currency
	where
		b.quote_currency = \\'USD\\'
		and a.userid not in (18144,17626)
	union
	select
		m.userid user_id,
		m.assettypeid asset,
		cast(m.balance as decimal(36,8)) amount,
		(cast(m.balance as decimal(36,8))/cast(c.buy_price as decimal(36,8))) as usd_amount,
		cast(c.buy_price as decimal(36,8)) rate
	from
		lemoncash_ar.wallets as m
	inner join
		lemoncash_ar.exchangeratesv2 c on m.assettypeid = c.quote_currency
	where
		m.assettypeid = \\'MONEY\\'
		and m.userid not in (18144,17626)
)
select
	extract(year from getdate()) as year,
	extract(month from getdate()) as month,
	extract(day from getdate()) as day,
	extract(hour from getdate()) as hour,
	user_id,
	asset,
	amount,
	usd_amount,
	rate
from
	balances
order by
	year,
	month,
	day,
	hour,
	asset')
to 's3://data-airflow-tables-queries-production/tables/balances/'
iam_role 'arn:aws:iam::127071149305:role/RedshiftS3'
parquet
partition by (year, month, day, hour, asset)
allowoverwrite""")

		task_get_all_balances

# redshift://jonathan:mV>"*(Tn?A^uU9z`@lemon-bi.cmv65ijkzusw.sa-east-1.redshift.amazonaws.com:5439?role_arn=&region_name=sa-east-1