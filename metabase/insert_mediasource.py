from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta
import os

with DAG(
    dag_id="insert_mediasource", 
    start_date=days_ago(1), 
    schedule_interval='20 1 * * *', 
    tags=['tabla']) as dag:

    truncate_mediasource = RedshiftSQLOperator(
        task_id='truncate_mediasource', 
        sql=""" truncate lemoncash_data.users_mediasource """)    

    insert_mediasource = RedshiftSQLOperator(
        task_id='insert_mediasource', 
        sql=""" INSERT INTO lemoncash_data.users_mediasource (select u.customer_user_id,
                    u.createdat,
                   u.media_source,
                   c.CAC_kyc,
                   a.amount as ubi
            from (select media_source,
                           a.mes,
                           cast(costo as decimal(32,8))/user_id as CAC_kyc
                    from 
                        (select count(distinct customer_user_id) as user_id, 
                                media_source, 
                                date_trunc('month',createdat) as mes 
                        from 
                                (select createdat, customer_user_id, a.id, media_source,af_channel
                                from lemoncash_ar.users a 
                                
                                inner join 
                                 (
                                 
                                    (
                                     SELECT  
                                         distinct customer_user_id,
                                         coalesce(media_source, af_prt) as media_source,
                                         af_channel
                                     FROM 
                                         lemoncash_dynamo.appsflyer_android e
                                    
                                     WHERE 
                                        (event_name = 'KycValidatedEvent' 
                                            or (event_name = 'tx_kycApproved' 
                                             and event_time::timestamp - interval '3 hour' >= '2022-06-15'::timestamp with time zone)
                                        )
                                        AND event_time > '2022-02-01'
                                     order by 1
                                    
                                    )
                                
                                UNION ALL 
                                  
                                    (
                                    SELECT  
                                         distinct customer_user_id,
                                         coalesce(media_source, af_prt) as media_source,
                                         af_channel
                                     FROM 
                                         lemoncash_dynamo.appsflyer_ios e
                                    
                                     WHERE 
                                        (
                                        event_name = 'KycValidatedEvent' or 
                                         (event_name = 'tx_kycApproved' and event_time::timestamp - interval '3 hour' >= '2022-06-15'::timestamp with time zone)
                                        )
                                        AND event_time > '2022-02-01'
                                     order by 1
                                    )
                                ) b on a.id = b.customer_user_id
                        )
                        group by 2,3 ) a 
                    inner join 
                        (
                        select mes,media,costo 
                        from gsheets.cost_costo_mediasource_mensual_hoja_1
                        ) b on a.media_source = b.media and a.mes = b.mes
                    WHERE a.mes > '2022-02-01'
                    ORDER BY media_source,a.mes
                    
                    )c

            inner join (select createdat, customer_user_id, a.id, media_source,af_channel
                        from lemoncash_ar.users a 
                        
                        inner join 
                         (
                         
                            (
                             SELECT  
                                 distinct customer_user_id,
                                 coalesce(media_source, af_prt) as media_source,
                                 af_channel,
                                 date_trunc('month', cast(event_time as timestamp)) as mes
                             FROM 
                                 lemoncash_dynamo.appsflyer_android e
                            
                             WHERE 
                                (event_name = 'KycValidatedEvent' 
                                    or (event_name = 'tx_kycApproved' 
                                     and event_time::timestamp - interval '3 hour' >= '2022-06-15'::timestamp with time zone)
                                )
                                AND event_time >= '2022-03-01'
                             order by 1
                            
                            )
                        
                        UNION ALL 
                          
                            (
                            SELECT  
                                 distinct customer_user_id,
                                 coalesce(media_source, af_prt) as media_source,
                                 af_channel,
                                 date_trunc('month', cast(event_time as timestamp)) as mes
                             FROM 
                                 lemoncash_dynamo.appsflyer_ios e
                            
                             WHERE 
                                (
                                event_name = 'KycValidatedEvent' or 
                                 (event_name = 'tx_kycApproved' and event_time::timestamp - interval '3 hour' >= '2022-06-15'::timestamp with time zone)
                                )
                                AND event_time > '2022-02-01'
                             order by 1
                            )
                        ) b on a.id = b.customer_user_id
                )u
                on c.mes = date_trunc('month', u.createdat) and u.media_source = c.media_source
                
                left join (select user_id, cast(amount as decimal(32,8))*buy_price as amount
                            from lemoncash_ar.activities a 
                                 inner join (SELECT DATE_TRUNC('hour', created_at) AS fechacotizacion,
                                                    AVG(cast(purchase_price_amount as decimal(32,8))) AS buy_price
                                             FROM  lemoncash_ar.cryptopurchasetransactions
                                             where purchase_currency = 'BTC'
                                             GROUP BY 1) b 
                                 ON  DATE_TRUNC('hour', a.createdat) = b.fechacotizacion
                            where transaction_type = 'REWARD_ACQUISITION_GIFT')a
                ON u.customer_user_id = a.user_id

            ORDER BY 1) """)
    truncate_mediasource >> insert_mediasource