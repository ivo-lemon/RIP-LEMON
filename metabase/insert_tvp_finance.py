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
    dag_id="insert_tvp_finance", 
    start_date=days_ago(1), 
    schedule_interval='0 3 * * *', 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla']) as dag:

    truncate_tvp_finance = RedshiftSQLOperator(
        task_id='truncate_tvp_finance', 
        sql="TRUNCATE lemoncash_data.tvp_finance;"
    )
    

    insert_tvp_finance = RedshiftSQLOperator(
        task_id='insert_tvp_finance', 
        sql=""" INSERT INTO lemoncash_data.tvp_finance (select hora, transaction_type, sum(volumen) as volumen_hora, sum(cantidad_hora) as cantidad_hora
from
    (select hora, volumen, case when transaction_type = 'CRYPTO_SALE' and visible = 1 then 'CRYPTO_SALE'
                                        when transaction_type = 'CRYPTO_SALE' and visible = 0 then 'AUTOSWAP'
                                        else transaction_type end as transaction_type,
            cantidad_hora
    from
    
        ((  SELECT
                date_trunc('day',mes) as hora,
                transaction_type,
                --user_id,
                visible,
                SUM(CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8))) AS Volumen,
                count(distinct transaction_id) as cantidad_hora
            
            FROM
                    (
                        SELECT
                            a.transaction_id,
                            a.transaction_type,
                            a.user_id,
                            a.currency,
                            a.visible,
                            DATE_TRUNC('hour', a.createdAt) AS mes,
                            SUM(CAST(a.amount AS DECIMAL(36,8))) AS suma_mes
                        
                        FROM
                            lemoncash_ar.activities a inner join lemoncash_ar.users u
                        ON
                            a.user_id = u.id
                        
                        WHERE
                            a.updated_by_id IS NULL
                            AND (a.state = 'DONE' and transaction_type = 'CRYPTO_PURCHASE')
                            AND a.user_id NOT IN (18144,17626,1343730, 1343764)
                            --and operation_type = 'DEBIT'
                            and operation_country = 'ARG'
                            
                        GROUP BY 1,2,3,4,5,6
                        
                    ) a
                INNER JOIN
                (
                    (
                    SELECT
                        DATE_TRUNC('hour', a.fecha) AS fecha,
                        a.currency,
                        AVG(CAST(p.purchase_price_amount AS DECIMAL(36,8))) AS rate_to_usd
                    
                    FROM
                        (
                            SELECT
                                DISTINCT (DATE_TRUNC('hour', created_at)) AS fecha,
                                purchase_currency AS currency
                            
                            FROM
                                lemoncash_ar.cryptopurchasetransactions
                            
                            ORDER BY fecha
                        ) a
                        INNER JOIN lemoncash_ar.cryptopurchasetransactions p ON a.fecha = (DATE_TRUNC('hour', p.created_at)) AND a.currency = p.purchase_currency
                    
                    GROUP BY 1,2
                    ORDER BY 1
                    )
                ) c ON a.mes = c.fecha AND a.currency = c.currency
            GROUP BY 1,2,3
            ORDER BY 1,3)
        
    union all
            (        SELECT
                        date_trunc('day',mes) as mes,
                        transaction_type,
                        visible,
                        SUM(CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8))) AS Volumen,
                        count(distinct transaction_id) as cantidad_hora
                    
                    FROM
                            (
                                SELECT
                                    a.transaction_id,
                                    a.transaction_type,
                                    a.currency,
                                    a.visible,
                                    DATE_TRUNC('hour', a.createdAt) AS mes,
                                    SUM(CAST(a.amount AS DECIMAL(36,8))) AS suma_mes
                                
                                FROM
                                    lemoncash_ar.activities a inner join lemoncash_ar.users u
                                ON
                                    a.user_id = u.id
                                
                                WHERE
                                    a.updated_by_id IS NULL
                                    AND (a.state = 'DONE' OR (a.state = 'PENDING' and a.transaction_type IN ('WALLET_TO_EXTERNAL_WALLET', 'VIRTUAL_WITHDRAWAL', 'WALLET_TO_BANK') ) )
                                    AND a.user_id NOT IN (18144,17626,1343730, 1343764)
                                    AND (a.transaction_type not in ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','REWARD_LEMON_CARD_CASHBACK','LEMON_CARD_PAYMENT') 
                                        OR (a.transaction_type = 'CRYPTO_SALE' AND a.operation_type = 'DEBIT')
                                        OR (a.transaction_type = 'CRYPTO_SWAP' AND a.operation_type = 'DEBIT'))
                                    and operation_country = 'ARG'
                                    
                                GROUP BY 1,2,3,4,5
                                
                            ) a
                        INNER JOIN
                        (
                            (
                            SELECT
                                DATE_TRUNC('hour', a.fecha) AS fecha,
                                a.currency,
                                AVG(CAST(p.crypto_sale_price_amount AS DECIMAL(36,8))) AS rate_to_usd
                            
                            FROM
                                (
                                    SELECT
                                        DISTINCT (DATE_TRUNC('hour', created_at)) AS fecha,
                                        currency AS currency
                                    
                                    FROM
                                        lemoncash_ar.cryptosaletransactions
                                    
                                    ORDER BY fecha
                                ) a
                                INNER JOIN lemoncash_ar.cryptosaletransactions p ON a.fecha = (DATE_TRUNC('hour', p.created_at)) AND a.currency = p.currency
                            GROUP BY 1,2
                            ORDER BY 1
                            )

                            UNION ALL

                            (
                                SELECT
                                    DATE_TRUNC('hour', a.fecha) AS fecha,
                                    a.currency,
                                    AVG(1/CAST(p.usd_sale_price_amount AS DECIMAL(36,8))) AS rate_to_usd
                                
                                FROM
                                    (
                                        SELECT
                                            DISTINCT (DATE_TRUNC('hour', created_at)) AS fecha,
                                            'MONEY' AS currency
                                        
                                        FROM
                                            lemoncash_ar.cryptosaletransactions
                                        
                                        ORDER BY fecha
                                    ) a
                                    INNER JOIN lemoncash_ar.cryptosaletransactions p ON a.fecha = (DATE_TRUNC('hour', p.created_at))
                                
                                GROUP BY 1,2
                                ORDER BY 1
                            )
                        ) c ON a.mes = c.fecha AND a.currency = c.currency
                    GROUP BY 1,2,3
                    ORDER BY 1,3)
                    
            UNION ALL
            
                (select dia as day,
                    'LEMON_CARD_PAYMENT' as transaction_type, 
                    1 as visible,
                    case when ccl > 0 then (sum((a.vol_internacional*coalesce(coalesce(tc_factura,billete_venta),0))/(ccl)) + sum((a.vol_nacional/ccl)))
                        else 0 end as volumen,
                    sum(tx_nacional + tx_internacional) as cantidad_hora
                from
                    (select 
                        date_trunc('day', (fecha_de_presentaci_n::date) - '1 Day'::interval) as dia, 
                        sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 and signo_importe_compensaci_n = '+' then cast(importe_de_compensaci_n as decimal(36,8))
                                when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 and signo_importe_compensaci_n = '-' then cast(importe_de_compensaci_n as decimal(36,8)) * (-1)
                                else 0 end) as vol_nacional, 
                        sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 and signo_importe_compensaci_n = '+' then cast(importe_de_compensaci_n as decimal(36,8))
                                when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 and signo_importe_compensaci_n = '-' then cast(importe_de_compensaci_n as decimal(36,8)) * (-1)
                                else 0 end) as vol_internacional,
                        sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 then 1
                                when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 then 1
                                else 0 end) as tx_nacional, 
                        sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 then 1
                                when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 then 1
                                else 0 end) as tx_internacional
                    from 
                        liquidaciones_conci.t2001d
                    group by 1
                    order by 1 desc
                    ) a
                full join gsheets.cotizaciones_finance_mensuales_hoja_1 c on date_trunc('month',a.dia) = c.mes
                full join (select fecha, comprador as billete_compra, vendedor as billete_venta
                            from lemoncash_data.oficial_tc_bna
                            where fecha is not null) d on a.dia = d.fecha
                where ccl > 0
                group by 1,2,3,ccl
                order by 1 desc
                )
                    
                order by 1 desc,2)
    
    order by 1 desc, 2 )
where hora is not null
group by 1,2
order by 1 desc, 2)  """)

    
    truncate_tvp_finance >> insert_tvp_finance