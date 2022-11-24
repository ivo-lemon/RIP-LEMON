from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sql import SQLCheckOperator
from datetime import timedelta,datetime
import os
from plugins import slack_util
CHANNEL = "#airflow-monitors"
OWNERS= ["U035P7MR3UZ"]

with DAG(
    dag_id="insert_exchange_rate_usd", 
    start_date=datetime(2022, 10, 19), 
    schedule_interval='10 * * * *', 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla']) as dag:

    create_staging = RedshiftSQLOperator(
        task_id = 'create_staging',
        sql = """ create table lemoncash_data.staging_rates(
            "fecha" datetime,
            "currency" varchar,
            "rate_sale" decimal(36,8),
            "rate_purchase" decimal(36,8)
        ); """
    )

    create_staging_2 = RedshiftSQLOperator(
        task_id = 'create_staging_2',
        sql = """ create table lemoncash_data.staging_rates_2(
            "fecha" datetime,
            "currency" varchar,
            "rate_sale" decimal(36,8),
            "rate_purchase" decimal(36,8)
        ); """
    )

    insert_exchange_rate_usd = RedshiftSQLOperator(
        task_id='insert_exchange_rate_usd_in_staging', 
        sql=""" INSERT INTO lemoncash_data.staging_rates 
(select * from
    (select b.fecha, b.currency, 
        case when a.rate_sale is not null then a.rate_sale else last_value(a.rate_sale ignore nulls) over (partition by b.currency order by b.fecha asc rows unbounded preceding) end as rate_sale,
        case when a.rate_purchase is not null then a.rate_purchase else last_value(a.rate_purchase ignore nulls) over (partition by b.currency order by b.fecha asc rows unbounded preceding) end as rate_purchase
    from
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
        ) a
    
    full join
    
        (select distinct date_trunc('hour',a.createdat) as fecha, b.currency
        from lemoncash_ar.activities a, (select distinct currency from lemoncash_ar.activities) b
        order by 1 desc) b
        
    on a.currency = b.currency and a.fecha = b.fecha
    order by 1 desc, 2)
where 1=1 
    and rate_sale is not null
order by 1 desc, 2
)
 """)

    insert_in_staging_2 = RedshiftSQLOperator(
        task_id = 'insert_in_staging_2',
        sql = """ 
            INSERT INTO LEMONCASH_DATA.STAGING_RATES_2
            (select *
            from lemoncash_data.staging_rates
            where fecha not in (select fecha from lemoncash_data.exchange_rate_usd)
            )
        """
    )

    insert_prod = RedshiftSQLOperator(
       task_id = 'insert_staging_in_prod',
       sql = """ insert into lemoncash_data.exchange_rate_usd (select * from lemoncash_data.staging_rates_2) """
    )

    check_wrong_dates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_wrong_dates",
    sql="""WITH stage_table as (select 1 as test,max(fecha) as max_date from lemoncash_data.staging_rates_2),
       prod_table as (select 1 as test, max(fecha) as max_date from lemoncash_data.exchange_rate_usd)
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
       select fecha, currency, rate_sale, rate_purchase, COUNT(*)
       from lemoncash_data.staging_rates_2
       group by 1,2,3,4
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
                from lemoncash_data.staging_rates_2
                where fecha is null or currency is null or rate_sale is null or rate_purchase is null
                )
            )
        select case when null_events > 0 then 0 else 1 end as null_check
        from null_rows
    """
    )

    delete_staging = RedshiftSQLOperator(
        task_id="delete_staging", 
        sql= """ DROP TABLE lemoncash_data.staging_rates
            """,
        trigger_rule="all_done"
        )

    delete_staging_2 = RedshiftSQLOperator(
        task_id="delete_staging_2", 
        sql= """ DROP TABLE lemoncash_data.staging_rates_2
            """,
        trigger_rule="all_done"
        )

    
    [create_staging,create_staging_2]>>insert_exchange_rate_usd>>insert_in_staging_2>>[check_duplicates,check_nulls,check_wrong_dates]>>insert_prod>>[delete_staging,delete_staging_2]