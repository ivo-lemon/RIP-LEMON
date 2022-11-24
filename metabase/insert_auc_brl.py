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
    dag_id="insert_auc_brl", 
    start_date=datetime(2022,10,5,12,30), 
    schedule_interval='5 4 * * *', 
    tags=['tabla', 'brasil', 'auc']) as dag:

    insert_auc_brl = RedshiftSQLOperator(
        task_id='insert_auc_brl', 
        sql=""" INSERT INTO snapshots.auc 
(select assettypeid as asset,
    sum(cast(balance as decimal(36,8))) as balance,
    (select cast(rate_purchase as decimal(38,20)) from (select rate_purchase, max(fecha) from lemoncash_data.exchange_rate_usd where currency = 'BRL' group by 1 order by 2 desc limit 1)) as rate,
    (select cast((sell_price+cast(buy_price as decimal(38,20)))/2 as decimal(38,20)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY' ) as price_usd,
    sum(cast(balance as decimal(36,8)) / cast(buy_price as decimal(36,8))) as balance_usd,
    sum(cast(balance as decimal(36,8)) / cast(buy_price as decimal(36,8)))*(select cast(buy_price as decimal(36,8)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY') as balance_ars,
    current_timestamp as fecha,
    count(distinct a.userid) as users
    
from lemoncash_ar.wallets a 
inner join lemoncash_ar.exchangeratesv2 b on a.assettypeid = b.quote_currency 
where assettypeid = 'BRL' and a.userid not in (18144,17626,1343730,1343764) and cast(balance as decimal(36,8)) > 0
group by 1

) """)

insert_auc_brl