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
    dag_id="insert_snapshot_auc", 
    start_date=datetime(2022,10,21), 
    schedule_interval='15 5 * * *', 
    tags=['tabla', 'auc']) as dag:


    insert_snapshot_auc = RedshiftSQLOperator(
        task_id='insert_snapshot_auc', 
        sql=""" INSERT INTO snapshots.auc 
(select asset, balance,rate, price_usd, balance_ars, balance_usd, fecha, users, country
from((
select tabla.assettypeid as asset, 
operation_country as country,
sum(cast(tabla.balance as decimal(36,8))) as balance, 
(select cast((sell_price+cast(buy_price as decimal(36,8)))/2 as decimal(36,8)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY' ) as price_usd, 
sum(tabla.usd) as balance_usd, 
sum(case when assettypeid = 'MONEY' then cast(tabla.balance as decimal(36,8)) 
else tabla.usd*(select cast(buy_price as decimal(36,8)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY' ) 
end) as balance_ars,
current_timestamp as fecha,
cast(count(distinct tabla.userid) as decimal(36,8)) as users,
null as rate

from ((select cast(a.balance as decimal(36,8)) as balance, 
    a.assettypeid, 
    userid,
    operation_country,
    (cast(a.balance as decimal(36,8))*cast(b.buy_price as decimal(36,8))) as usd, 
    cast(b.buy_price as decimal(36,8))
    from lemoncash_ar.wallets a 
    inner join lemoncash_ar.users u on a.userid = u.id
    inner join lemoncash_ar.exchangeratesv2 b on a.assettypeid = b.base_currency 
    where (b.quote_currency = 'USD' OR b.quote_currency = 'MONEY' OR b.quote_currency = 'BRL')
            and a.userid not in (18144,17626,1343730,1343764) 
            and cast(a.balance as decimal(36,8)) > 0)

union all (select cast(m.balance as decimal(36,8)) as balance, 
    m.assettypeid, 
    userid,
    operation_country,
    (cast(m.balance as decimal(36,8))/cast(c.buy_price as decimal(36,8))) as usd, 
    cast(c.buy_price as decimal(36,8)) 
    from lemoncash_ar.wallets as m 
    inner join lemoncash_ar.users u on m.userid = u.id
    inner join lemoncash_ar.exchangeratesv2 c on m.assettypeid = c.quote_currency 
    where m.assettypeid = 'MONEY' and m.userid not in (18144,17626,1343730,1343764) and cast(m.balance as decimal(36,8)) > 0)) as tabla 

group by 1,2,4
order by 1 
)

union all 
(select assettypeid as asset,
    operation_country as country,
    sum(cast(balance as decimal(36,8))) as balance,
    (select cast((sell_price+cast(buy_price as decimal(36,8)))/2 as decimal(36,8)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY' ) as price_usd,
    sum(cast(balance as decimal(36,8)) / cast(buy_price as decimal(36,8))) as balance_usd,
    sum(case when assettypeid = 'MONEY' then cast(balance as decimal(36,8))
    else (cast(balance as decimal(36,8)) / cast(buy_price as decimal(36,8)))*(select cast(buy_price as decimal(36,8)) from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY') 
    end) as balance_ars,
    current_timestamp as fecha,
   cast(count(distinct a.userid) as decimal(36,8)) as users,
   null as rate
    
from lemoncash_ar.wallets a 
inner join lemoncash_ar.exchangeratesv2 b on a.assettypeid = b.quote_currency 
inner join lemoncash_ar.users u on a.userid = u.id

where a.userid not in (18144,17626,1343730,1343764)
    and cast(balance as decimal(36,8)) > 0 and assettypeid = 'BRL'

group by 1,2
order by 1 
))
order by 1, 8)
 """)

insert_snapshot_auc
