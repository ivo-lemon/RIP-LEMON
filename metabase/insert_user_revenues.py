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
    dag_id="insert_user_revenues", 
    start_date=days_ago(1), 
    schedule_interval='59 23 */1 * *', 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla']) as dag:

    truncate_user_revenues = RedshiftSQLOperator(
        task_id='truncate_user_revenues', 
        sql="TRUNCATE lemoncash_data.user_revenues;"
    )
    

    insert_user_revenues = RedshiftSQLOperator(
        task_id='insert_user_revenues', 
        sql=""" INSERT INTO lemoncash_data.user_revenues 
(with B as (
    SELECT SUM(fee_amount::decimal(36,8)*coalesce(rate_purchase,rate_sale)::decimal(36,8)) as fee_amount_usd,
                cast(CASE WHEN concept_code = 'CRYPTO_PURCHASE' THEN 'CRYPTO_PURCHASE'
                    END as varchar(30)) as concept_code,
                DATE_TRUNC('day',created_at - '3 hour'::interval) as dia,
                user_id         --, fee_amount, rate_purchase --borrarlos despues
                
        FROM lemoncash_ar.feerecords fr 
        
        INNER JOIN lemoncash_ar.activities a 
            ON a.transaction_id = fr.transaction_id and a.operation_type = 'DEBIT'
        
        LEFT JOIN lemoncash_data.exchange_rate_usd b on a.currency = b.currency and date_trunc('hour',a.createdat) = b.fecha
        where 1=1 --and a.user_id = 
        and a.operation_type = 'DEBIT'
        GROUP BY 2,3,4   -- ,5,6 --borrarlos dospues
        ORDER BY 4,3
    ), A as 

        (with a as 
                (SELECT
                    case when date_trunc('day', fecha_de_presentaci_n::date - '1 day'::interval) < date_trunc('day','2022-05-01'::date) and signo_importe_compensaci_n = '+'
                            then sum(CONVERT(DECIMAL(16, 8), ISNULL(NULLIF(arancel_emisor, ''), '0'))) * 0.7
                        when date_trunc('day', fecha_de_presentaci_n::date - '1 day'::interval) >= date_trunc('day','2022-05-01'::date) and signo_importe_compensaci_n = '+'
                            then sum(CONVERT(DECIMAL(16, 8), ISNULL(NULLIF(arancel_emisor, ''), '0'))) * 0.9 
                        when date_trunc('day', fecha_de_presentaci_n::date - '1 day'::interval) < date_trunc('day','2022-05-01'::date) and signo_importe_compensaci_n = '-'
                            then sum(CONVERT(DECIMAL(16, 8), ISNULL(NULLIF(arancel_emisor, ''), '0'))) * 0.7 * -1
                            else sum(CONVERT(DECIMAL(16, 8), ISNULL(NULLIF(arancel_emisor, ''), '0'))) * 0.9 * -1
                            end AS fee_amount,
                    case when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 then 'MONEY' 
                        when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 then 'USD' end as currency,
                    cast('LEMON_CARD_PAYMENT'as varchar(30)) as concept_code ,
                    date_trunc('day', fecha_de_presentaci_n::date - '1 day'::interval) as dia,
                    owner_id as user_id
                FROM
                    liquidaciones_conci.t2001d a
                INNER JOIN
                    lemoncash_ar.lemoncardpaymentauthorizationrequests b
                ON
                    a.request_id_de_la_autorizacion = b.external_request_id
                INNER JOIN
                    lemoncash_ar.cardaccounts c
                ON
                    c.id = b.card_account_id
                INNER JOIN
                    lemoncash_ar.accounts d
                ON
                    d.id = c.user_account_id
                WHERE arancel_emisor is not null
                    and arancel_emisor <> ''
                    and arancel_emisor > 0
                GROUP BY 2,3, fecha_de_presentaci_n, arancel_emisor, signo_importe_compensaci_n, owner_id
                ORDER BY 3,2),
            b as (select *
                from gsheets.cotizaciones_finance_mensuales_hoja_1),
            c as (select fecha, comprador as billete_compra, vendedor as billete_venta
                    from lemoncash_data.oficial_tc_bna
                    where fecha is not null)
            SELECT  
                    SUM(case when a.currency = 'USD' then (fee_amount*coalesce(coalesce(tc_factura,c.billete_venta),0))/ccl
                            when a.currency = 'MONEY' then fee_amount/ccl end) as fee_amount_usd,
                        concept_code,
                        dia,
                        user_id
            FROM a
            full JOIN b 
                on date_trunc('month', a.dia) = b.mes
            full join c
                on date_trunc('day', c.fecha) = a.dia
            where mes >= '2022-01-01'
            group by 2,3,4
            order by 4 desc),
    
    C as (SELECT SUM(CASE WHEN fee_currency in ('MONEY','BRL') then fee_amount::decimal(36,8)*coalesce(rate_sale,rate_purchase)::decimal(36,8)
                    ELSE fee_amount::decimal(36,8)*rate_sale::decimal(36,8)
                    END) as fee_amount_usd,
                cast(CASE 
                    WHEN concept_code = 'CRYPTO_SALE' THEN 'CRYPTO_SALE'
                    --WHEN concept_code = 'CRYPTO_SWAP' THEN 'CRYPTO_SWAP'
                    --ELSE 'OTHER'
                    END as varchar(30)) as concept_code,
                DATE_TRUNC('day',created_at - '3 hour'::interval) as dia,
                user_id     --, fee_amount, rate_sale --borrarlos despues
                
        FROM lemoncash_ar.feerecords fr 
        
        INNER JOIN lemoncash_ar.activities a 
            ON a.transaction_id = fr.transaction_id and a.operation_type = 'CREDIT' --and a.user_id =  and concept_code = 'CRYPTO_SALE'
        
        LEFT JOIN lemoncash_data.exchange_rate_usd b on a.currency = b.currency and date_trunc('hour',a.createdat) = b.fecha
        GROUP BY 2,3,4     --,5,6 --borrarlos dospues
        ORDER BY 4,3),

    D as (SELECT SUM(CASE WHEN fee_currency in ('MONEY','BRL') then fee_amount::decimal(36,8)/coalesce(rate_sale,rate_purchase)::decimal(36,8)
                    ELSE fee_amount::decimal(36,8)*rate_sale::decimal(36,8)
                    END) as fee_amount_usd,
                cast(CASE 
                    WHEN concept_code = 'CRYPTO_SWAP' THEN 'CRYPTO_SWAP'
                    END as varchar(30)) as concept_code,
                DATE_TRUNC('day',created_at - '3 hour'::interval) as dia,
                user_id      --, fee_amount, rate_sale --borrarlos despues
                
        FROM lemoncash_ar.feerecords fr 
        
        INNER JOIN lemoncash_ar.activities a 
            ON a.transaction_id = fr.transaction_id and a.operation_type = 'DEBIT' --and a.user_id = 
        
        LEFT JOIN lemoncash_data.exchange_rate_usd b on a.currency = b.currency and date_trunc('hour',a.createdat) = b.fecha
        GROUP BY 2,3,4
        ORDER BY 4,3)
        
    SELECT user_id, date_trunc('day',dia) as dia, 
        sum(case when concept_code = 'LEMON_CARD_PAYMENT' then fee_amount_usd else 0 end) as revenue_card,
        sum(case when concept_code = 'CRYPTO_PURCHASE' then fee_amount_usd else 0 end) as revenue_purchase,
        sum(case when concept_code = 'CRYPTO_SWAP' then fee_amount_usd else 0 end) as revenue_swap,
        sum(case when concept_code = 'CRYPTO_SALE' then fee_amount_usd else 0 end) as revenue_sale,
        sum(fee_amount_usd) as revenue_total
    
    from (
        select B.* from B
        union all
        select A.* from A     
        union all 
        select C.* from C
        union all
        select D.* from D
    )
    where concept_code is not null
    --and user_id = 
    group by 1,2
    ORDER BY 2 desc,1)
        """)

    
    truncate_user_revenues >> insert_user_revenues


