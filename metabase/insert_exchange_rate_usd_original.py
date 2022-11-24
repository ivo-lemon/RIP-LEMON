from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta,datetime
import os
from plugins import slack_util
CHANNEL = "#airflow-monitors"
OWNERS= ["U035P7MR3UZ"]

with DAG(
    dag_id="insert_exchange_rate_usd_original", 
    start_date=datetime(2022, 10, 19), 
    schedule_interval='10 8 * * *',
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ), 
    tags=['tabla']) as dag:

    truncate_exchange_rate_usd = RedshiftSQLOperator(
        task_id='truncate_exchange_rate_usd_og', 
        sql="TRUNCATE bi_tools.exchange_rate_usd_original;"
    )
    

    insert_exchange_rate_usd = RedshiftSQLOperator(
        task_id='insert_exchange_rate_usd_og', 
        sql=""" INSERT INTO bi_tools.exchange_rate_usd_original 
(select distinct a.fecha, a.currency, rate_sale, rate_purchase
from
    (select distinct date_trunc('hour',a.createdat) as fecha, b.currency
    from lemoncash_ar.activities a, (select distinct currency from lemoncash_ar.activities) b
    order by 1 desc) a
    
    full join
    
    (
        (
            select date_trunc('hour',a.fecha) as fecha, a.currency, avg(coalesce(b.promedio_moneda,c.promedio_moneda)) as rate_sale, avg(coalesce(c.promedio_moneda,b.promedio_moneda)) as rate_purchase
            from 
                ( 
                    select distinct date_trunc('hour',createdat) as fecha, currency 
                    from lemoncash_ar.activities
                    where currency != 'MONEY' -- and currency != 'BRL' and currency != 'JUICE' AND currency != 'LUNA' AND currency != 'UST'
                    group by 1,2
                ) a 
            left join 
                (
                    select avg(cast(crypto_sale_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('hour',created_at) as fecha, currency 
                    from lemoncash_ar.cryptosaletransactions a inner join lemoncash_ar.users b on a.user_id = b.id 
                    group by 2,3 
                ) b on a.fecha = b.fecha and a.currency = b.currency
                
            left join 
                
                (
                    select avg(cast(purchase_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('hour', created_at) as fecha, purchase_currency 
                    from lemoncash_ar.cryptopurchasetransactions a 
                    group by 2,3 
                ) c on a.fecha = c.fecha and a.currency = c.purchase_currency
            WHERE b.promedio_moneda is not null or c.promedio_moneda is not null
            group by 1,2
            ORDER BY 1 ASC 
        )
        union all 
        -- PRECIO BRL 
        (
            select a.fecha, a.currency, 
                case when coalesce(b.promedio_moneda,c.promedio_moneda) is null then coalesce(d.promedio_moneda, 0.2) else coalesce(b.promedio_moneda,c.promedio_moneda) end as rate_sale,
                case when coalesce(c.promedio_moneda,b.promedio_moneda) is null then coalesce(d.promedio_moneda, 0.2) else coalesce(c.promedio_moneda,b.promedio_moneda) end as rate_purchase
            from 
                (select distinct date_trunc('hour',a.createdat) as fecha, b.currency
                from lemoncash_ar.activities a, (select distinct currency from lemoncash_ar.activities where currency = 'BRL') b
                order by 1 desc) a 
            left join 
                (
                    select 1/avg(cast(usd_sale_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('hour',created_at) as fecha, usd_sale_price_currency 
                    from lemoncash_ar.cryptosaletransactions a inner join lemoncash_ar.users b on a.user_id = b.id 
                    where usd_sale_price_currency = 'BRL'
                    group by 2,3 
                ) b on a.fecha = b.fecha and a.currency = b.usd_sale_price_currency
                
            left join 
                
                (
                    select 1/avg(cast(usd_price_in_spending_fiat_currency as decimal(36,8))) as promedio_moneda, date_trunc('hour', created_at) as fecha, spending_money_currency 
                    from lemoncash_ar.cryptopurchasetransactions a 
                    where spending_money_currency = 'BRL'
                    group by 2,3 
                ) c on a.fecha = c.fecha and a.currency = c.spending_money_currency
            
            left JOIN
                (
                    select 1/avg(cast(usd_sale_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('day',created_at) as fecha, usd_sale_price_currency 
                    from lemoncash_ar.cryptosaletransactions a inner join lemoncash_ar.users b on a.user_id = b.id 
                    where usd_sale_price_currency = 'BRL'
                    group by 2,3 
                    order by 2
                ) d on d.fecha = date_trunc('day',b.fecha)
            where currency = 'BRL'
            ORDER BY 1 desc 
        )
        union all 
        -- PRECIO ARS
        (
            select a.fecha, a.currency, 
                case when coalesce(b.promedio_moneda,c.promedio_moneda) is null then d.promedio_moneda else coalesce(b.promedio_moneda,c.promedio_moneda) end as rate_sale,
                case when coalesce(c.promedio_moneda,b.promedio_moneda) is null then d.promedio_moneda else coalesce(c.promedio_moneda,b.promedio_moneda) end as rate_purchase
            from 
                ( 
                    select distinct date_trunc('hour',createdat) as fecha, currency 
                    from lemoncash_ar.activities
                    --where currency = 'MONEY' 
                    group by 1,2
                ) a 
            left join 
                (
                    select 1/avg(cast(usd_sale_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('hour',created_at) as fecha, usd_sale_price_currency 
                    from lemoncash_ar.cryptosaletransactions a inner join lemoncash_ar.users b on a.user_id = b.id 
                    where usd_sale_price_currency = 'MONEY'
                    group by 2,3 
                ) b on a.fecha = b.fecha and a.currency = b.usd_sale_price_currency
                
            left join 
                
                (
                    select 1/avg(cast(usd_price_in_spending_fiat_currency as decimal(36,8))) as promedio_moneda, date_trunc('hour', created_at) as fecha, spending_money_currency 
                    from lemoncash_ar.cryptopurchasetransactions a 
                    where spending_money_currency = 'MONEY'
                    group by 2,3 
                ) c on a.fecha = c.fecha and a.currency = c.spending_money_currency
            
            full JOIN    
                (
                    select 1/avg(cast(usd_sale_price_amount as decimal(36,8))) as promedio_moneda, date_trunc('week',created_at) as fecha, usd_sale_price_currency 
                    from lemoncash_ar.cryptosaletransactions a inner join lemoncash_ar.users b on a.user_id = b.id 
                    where usd_sale_price_currency = 'MONEY'
                    group by 2,3 
                ) d on d.fecha = date_trunc('week',b.fecha)
            ORDER BY 1 desc, 2)
    ) b
on a.fecha = b.fecha and a.currency = b.currency
where (b.currency = 'BRL' or rate_sale != 0.2)
and rate_sale is not null and rate_purchase is not null
order by 1 desc, 2
)
 """)

    
    truncate_exchange_rate_usd >> insert_exchange_rate_usd

