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
    dag_id="insert_daily_tvp", 
    start_date=days_ago(1), 
    schedule_interval='30 3 * * *', 
    tags=['tabla'],
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    ) as dag:

    truncate_tvp = RedshiftSQLOperator(
        task_id='truncate_tvp', 
        sql="TRUNCATE lemoncash_data.daily_tvp;"
    )
    
    

    insert_tvp = RedshiftSQLOperator(
        task_id='insert_tvp', 
        sql=""" INSERT INTO lemoncash_data.daily_tvp (select hora, transaction_type, sum(volumen) as volumen, sum(cantidad_hora) as cantidad_mes
from
    (select hora, user_id, volumen, case when transaction_type = 'CRYPTO_SALE' and visible = 1 then 'CRYPTO_SALE'
                                        when transaction_type = 'CRYPTO_SALE' and visible = 0 then 'AUTOSWAP'
                                        else transaction_type end as transaction_type,
            cantidad_hora
    from
    
        ((        SELECT
                    date_trunc('hour',mes) as hora,
                    transaction_type,
                    user_id,
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
                GROUP BY 1,2,3,4
                ORDER BY 1,3)
        
    union all
            (        SELECT
                        date_trunc('hour',mes) as mes,
                        transaction_type,
                        user_id,
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
                                    AND (a.state = 'DONE' OR (a.state = 'PENDING' and a.transaction_type IN ('WALLET_TO_EXTERNAL_WALLET', 'VIRTUAL_WITHDRAWAL', 'WALLET_TO_BANK') ) )
                                    AND a.user_id NOT IN (18144,17626,1343730, 1343764)
                                    and operation_country = 'ARG'
                                    AND (a.transaction_type not in ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','REWARD_LEMON_CARD_CASHBACK') 
                                        OR (a.transaction_type = 'CRYPTO_SALE' AND a.operation_type = 'DEBIT')
                                        OR (a.transaction_type = 'CRYPTO_SWAP' AND a.operation_type = 'DEBIT'))
                                    
                                GROUP BY 1,2,3,4,5,6
                                
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
                    GROUP BY 1,2,3,4
                    ORDER BY 1,3)
                order by 1 desc,2)
    
    order by 1 desc, 2 )

group by 1,2
order by 1 desc, 2) """)

    
    truncate_tvp >> insert_tvp

