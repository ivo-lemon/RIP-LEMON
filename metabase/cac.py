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
    dag_id="cac", 
    start_date=days_ago(1), 
    schedule_interval='30 2 * * 1', 
    tags=['tabla']) as dag: 

    insert_CAC_registro = RedshiftSQLOperator(
        task_id='insert_CAC_registro', 
        sql=""" INSERT INTO lemoncash_data.temp_CAC_registro (SELECT 
       media_source,
       cast(user_id as int) as user_id,
       cast(fecha_registro as timestamp) as fecha_registro,
       cast(costo_registro as decimal(32,8)) as costo_registro,
       cast(fecha_corrida as timestamp) as fecha_corrida
FROM ((SELECT 
       cast(user_id as int) user_id,
       cost.media_source,
       cast(fecha_registro as timestamp) as fecha_registro,
       cast(costo_registro as decimal(32,8)) as costo_registro,
       semana as fecha_corrida
       
FROM (SELECT semana,
       media_source,
       cast(spend as decimal(32,8))/registros as costo_registro
       
FROM (select date_trunc('week', date) as semana,
               media_source_pid,
               sum(case when media_source_pid = 'Twitter' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                         when media_source_pid = 'googleadwords_int' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                        when media_source_pid = 'bytedanceglobal_int' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                    else total_cost
                    end) as spend
               
        from (select date,
                   media_source_pid,
                   cast(case when total_cost = 'N/A' then '0'
                        else total_cost
                        end as decimal(32,2)) total_cost 
            from lemoncash_dynamo.hevo_costappsflyer 
            where campaign_c <> 'App_Retageting_LF_Android' and campaign_c <> 'Lemon_RE_AR' and media_source_pid not in ('sumate_influencer', 'telegram_ambassadors_personal_link')
            
            union all 
            
            select date,
                   media_source,
                   kwai 
            from gsheets.kawai_cost_cost)costo
        where semana = date_trunc('week', current_date) - interval '5 week'
        group by 1,2
        order by 1 desc, 2) cost
        INNER JOIN (SELECT  install.date,
                    case when  install.media_source = 'restricted' then 'Facebook Ads'
                                             else  install.media_source
                                             end as media_source,
                    count(distinct registro.user_id) as registros
            FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'tx_signUp' 
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    UNION ALL 
                    select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'tx_signUp' 
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week') registro
                    
                INNER JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'install' 
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    union all 
                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'install' 
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install
               ON registro.appsflyer_id = install.appsflyer_id
             GROUP BY 1,2) registros
    ON cost.media_source_pid = registros.media_source) cost 
    
    INNER JOIN (SELECT  distinct registro.user_id as user_id,
                        fecha_registro,
                        case when  install.media_source = 'restricted' then 'Facebook Ads'
                                    else  install.media_source
                                    end as media_source
        
                        
                    FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                 event_time as fecha_registro,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    UNION ALL 
                                    select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                        event_time as fecha_registro,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week') registro
                                    
                                INNER JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'install' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    union all 
                                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'install' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install
                               ON registro.appsflyer_id = install.appsflyer_id
    WHERE  install.media_source not in ('sumate_influencer', 'telegram_ambassadors_personal_link')
    order by user_id) users
        ON users.media_source = cost.media_source)
UNION ALL 
        (SELECT  distinct  cast( registro.user_id as int) as user_id,
                 install.media_source,
                 cast(fecha_registro as timestamp) as fecha_registro,
                 cast(0 as decimal(32,8)) as costo_registro,
                 registro.date
                        
                        
                    FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                  event_time as fecha_registro,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    UNION ALL 
                                    select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                        event_time as fecha_registro,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week') registro
                                    
                                INNER JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'install' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    union all 
                                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'install' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install
                               ON registro.appsflyer_id = install.appsflyer_id
        WHERE install.media_source = 'organic')))""")


    insert_CAC_RE = RedshiftSQLOperator(
        task_id='insert_CAC_RE', 
        sql=""" INSERT INTO lemoncash_data.CAC_RE (select cast(re_cac as decimal(32,8)) as re_cac,
cast(re_users as int) user_id,
type,
cast(fecha_corrida as timestamp) as fecha_corrida
       
from (select media_source_pid,
           cast(cost as decimal(32, 2))/RE_users as RE_CAC,
           semana as fecha_corrida
           
    from (select date_trunc('week', date) as semana,
                    media_source_pid,
                    sum(total_cost) as cost
                    
                    from (select date,
                               media_source_pid,
                               cast(case when total_cost = 'N/A' then '0'
                                    else total_cost
                                    end as decimal(32,2)) total_cost 
                        from lemoncash_dynamo.hevo_costappsflyer 
                        where (campaign_c = 'App_Retageting_LF_Android' or campaign_c = 'Lemon_RE_AR') and date >= '2022-07-01')costo
                    where semana = date_trunc('week', current_date) - interval '2 week'
            group by 1,2
            order by 1 desc, 2) cost_RE 
            
            INNER JOIN (SELECT  count(distinct registro.user_id) as RE_users,
                                case when registro.media_source = 'criteonew_int' or kyc.media_source = 'criteonew_int' then  'criteonew_int'
                                     when registro.media_source = 'liftoff_int' or kyc.media_source = 'liftoff_int' then 'liftoff_int'
                                end as media_source
                                                
                                            FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_android
                                                             where event_name = 'install')
                                                            union all 
                                                            (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_ios
                                                             where event_name = 'install' )) install
                                                        
                                            INNER JOIN ((select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                         event_time as fecha_registro,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_android
                                                             where event_name = 'tx_signUp' 
                                                            and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week')
                                                            UNION ALL 
                                                            select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                                event_time as fecha_registro,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_ios
                                                             where event_name = 'tx_signUp' 
                                                            and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week') registro
                                                            
                                                       ON registro.appsflyer_id = install.appsflyer_id
                                                       
                                            LEFT JOIN ((select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                         event_time as fecha_registro,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_android
                                                             where event_name = 'tx_kycApproved' 
                                                            and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week')
                                                            UNION ALL 
                                                            select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                                event_time as fecha_registro,
                                                                   coalesce(media_source, af_prt) as media_source,
                                                                   campaign,
                                                                   customer_user_id as user_id,
                                                                   appsflyer_id
                                                             from lemoncash_dynamo.appsflyer_ios
                                                             where event_name = 'tx_kycApproved' 
                                                            and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week') kyc
                                                            
                                                       ON kyc.appsflyer_id = install.appsflyer_id
                            WHERE (install.media_source <> 'criteonew_int' and install.media_source <> 'liftoff_int') and (registro.media_source = 'criteonew_int' or kyc.media_source = 'liftoff_int' or  kyc.media_source = 'criteonew_int' or registro.media_source = 'liftoff_int')
                            group by 2 ) usuarios
            ON usuarios.media_source = cost_RE.media_source_pid ) RE_cac 
            
            INNER JOIN (SELECT distinct registro.user_id as RE_users,
                            case when registro.media_source = 'criteonew_int' or kyc.media_source = 'criteonew_int' then  'criteonew_int'
                                 when registro.media_source = 'liftoff_int' or kyc.media_source = 'liftoff_int' then 'liftoff_int'
                            end as media_source,
                            case when registro.media_source = 'criteonew_int' or registro.media_source = 'liftoff_int' then 'RE_registro'
                                 when kyc.media_source = 'criteonew_int' or kyc.media_source = 'liftoff_int' then 'RE_kyc'
                                 end as type
                                 
                                            
                                        FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_android
                                                         where event_name = 'install')
                                                        union all 
                                                        (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_ios
                                                         where event_name = 'install' )) install
                                                    
                                        INNER JOIN ((select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                     event_time as fecha_registro,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_android
                                                         where event_name = 'tx_signUp' 
                                                        and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week')
                                                        UNION ALL 
                                                        select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                            event_time as fecha_registro,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_ios
                                                         where event_name = 'tx_signUp' 
                                                        and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week') registro
                                                        
                                                   ON registro.appsflyer_id = install.appsflyer_id
                                                   
                                        LEFT JOIN ((select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                     event_time as fecha_kyc,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_android
                                                         where event_name = 'tx_kycApproved' 
                                                        and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week')
                                                        UNION ALL 
                                                        select date_trunc('week', cast(event_time as timestamp)  - interval '3 hour' ) as date,
                                                            event_time as fecha_registro,
                                                               coalesce(media_source, af_prt) as media_source,
                                                               campaign,
                                                               customer_user_id as user_id,
                                                               appsflyer_id
                                                         from lemoncash_dynamo.appsflyer_ios
                                                         where event_name = 'tx_kycApproved' 
                                                        and date_trunc('week', cast(event_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '2 week') kyc
                                                        
                                                   ON kyc.appsflyer_id = install.appsflyer_id
                        WHERE (install.media_source <> 'criteonew_int' and install.media_source <> 'liftoff_int' ) and (registro.media_source = 'criteonew_int' or kyc.media_source = 'liftoff_int' or  kyc.media_source = 'criteonew_int' or registro.media_source = 'liftoff_int')) users
                    
                        ON users.media_source = RE_cac.media_source_pid)""")

    insert_CAC_kyc = RedshiftSQLOperator(
        task_id='insert_CAC_kyc', 
        sql=""" INSERT INTO lemoncash_data.temp_CAC_kyc(SELECT 
        media_source,
       cast(user_id as int) as user_id,
       cast(fecha_kyc as timestamp) as fecha_kyc,
       cast(costo_kyc as decimal(32,8)) as costo_kyc,
       cast(fecha_corrida as timestamp) as fecha_corrida
       
FROM ((SELECT cast(user_id as int) as user_id,
       cost.media_source,
       cast(fecha_kyc as timestamp) as fecha_kyc,
       cast(costo_kyc as decimal(32,8)) as costo_kyc,
       semana as fecha_corrida

       
FROM (SELECT semana,
       media_source,
       cast(spend as decimal(32,8))/kyc as costo_kyc
       
FROM (select date_trunc('week', date) as semana,
               media_source_pid,
               sum(case when media_source_pid = 'Twitter' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                         when media_source_pid = 'googleadwords_int' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                        when media_source_pid = 'bytedanceglobal_int' then (cast(total_cost as decimal(32,6))*(select cast(billete_venta as decimal) as usd FROM gsheets.tc_oficial_bna_base_tc_hist_rico
where fecha = (select max(fecha) from gsheets.tc_oficial_bna_base_tc_hist_rico where fecha is not null and fecha != 'Fuente: BNA')))/(select buy_price from lemoncash_ar.exchangeratesv2 where quote_currency = 'MONEY')
                    else total_cost
                    end) as spend
               
        from (select date,
                   media_source_pid,
                   cast(case when total_cost = 'N/A' then '0'
                        else total_cost
                        end as decimal(32,2)) total_cost 
            from lemoncash_dynamo.hevo_costappsflyer 
            where campaign_c <> 'App_Retageting_LF_Android' and campaign_c <> 'Lemon_RE_AR' and media_source_pid not in ('sumate_influencer', 'telegram_ambassadors_personal_link')
            
            union all 
            
            select date,
                   media_source,
                   kwai 
            from gsheets.kawai_cost_cost)costo
        where semana = date_trunc('week', current_date) - interval '5 week'
        group by 1,2
        order by 1 desc, 2) cost

        INNER JOIN (SELECT  coalesce(install.date, kyc.date) as date,
        case when  coalesce(install.media_source, kyc.media_source) = 'restricted' then 'Facebook Ads'
                    else  install.media_source
                    end as media_source,
        count(distinct kyc.user_id) as kyc

        
            FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'tx_kycApproved'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    UNION ALL 
                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'tx_kycApproved'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) kyc
                    
                LEFT JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'tx_signUp'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    union all 
                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'tx_signUp' 
                    and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install

               ON kyc.appsflyer_id = install.appsflyer_id
    GROUP BY 1,2) kyc
    ON cost.media_source_pid = kyc.media_source) cost 
    
    INNER JOIN (SELECT  distinct kyc.user_id as user_id,
             cast(fecha_kyc as date),
             case when  coalesce(install.media_source, kyc.media_source) = 'restricted' then 'Facebook Ads'
                    else  install.media_source
                    end as media_source
    
            FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           customer_user_id as user_id,
                           event_time as fecha_kyc,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'tx_kycApproved'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    UNION ALL 
                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           customer_user_id as user_id,
                           event_time as fecha_kyc,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'tx_kycApproved'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) kyc
                    
                LEFT JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_android
                     where event_name = 'tx_signUp'
                     and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                    union all 
                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                           coalesce(media_source, af_prt) as media_source,
                           campaign,
                           customer_user_id as user_id,
                           appsflyer_id
                     from lemoncash_dynamo.appsflyer_ios
                     where event_name = 'tx_signUp' 
                    and  (campaign not in  ('App_Retageting_LF_Android', 'Lemon_RE_AR') or campaign is null)
                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install

               ON kyc.appsflyer_id = install.appsflyer_id) users
        ON users.media_source = cost.media_source)
UNION ALL 
        (SELECT  distinct cast(kyc.user_id as int) as user_id,
                 install.media_source,
                 cast(cast(fecha_kyc as date)  as timestamp)as fecha_kyc,
                 cast(0 as decimal(32,8)) as costo_kyc,
                 kyc.date
                        
                        
                    FROM ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                 event_time as fecha_kyc,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'tx_kycApproved' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    UNION ALL 
                                    select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                        event_time as fecha_kyc,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'tx_kycApproved' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week') kyc
                                    
                                INNER JOIN ((select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_android
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')
                                    union all 
                                    (select date_trunc('week', cast(install_time as timestamp)  - interval '3 hour' ) as date,
                                           coalesce(media_source, af_prt) as media_source,
                                           campaign,
                                           customer_user_id as user_id,
                                           appsflyer_id
                                     from lemoncash_dynamo.appsflyer_ios
                                     where event_name = 'tx_signUp' 
                                    and date_trunc('week', cast(install_time as timestamp) - interval '3 hour') = (date_trunc('week', (current_timestamp - interval '3 hour'))) - interval '5 week')) install
                               ON kyc.appsflyer_id = install.appsflyer_id
        WHERE install.media_source = 'organic')))""")

delete_select_table = RedshiftSQLOperator(
      task_id = 'delete_select_table',
      sql = '''DELETE FROM lemoncash_data.cac'''
)

select_temp_tables = RedshiftSQLOperator(
      task_id = 'select_temp_tables',
      sql = '''INSERT INTO lemoncash_data.cac (
            WITH registro as (
                    SELECT tcr.user_id,
                                   register_date,
                                   tcr.media_source,
                                   register_cost as cac_registro,
                                   coalesce(rer.re_cac,0) as re_cac_registro,
                                   coalesce(ubi.amount,0) as cac_ubi_registro,
                                   register_cost + re_cac_registro + cac_ubi_registro as cac_registro_total
                                   
                            FROM lemoncash_data.temp_cac_registro tcr
                                LEFT JOIN (SELECT * FROM lemoncash_data.CAC_RE WHERE type = 'RE_registro')rer ON rer.re_users = tcr.user_id
                                LEFT JOIN (SELECT user_id, 
                                                  cast(amount as decimal(32,8))*cast(rate_sale as decimal(32,8)) as amount
                                           FROM lemoncash_ar.activities a 
                                           INNER JOIN lemoncash_data.exchange_rate_usd er ON er.currency = a.currency and er.fecha = date_trunc('hour', a.createdat) 
                                           WHERE transaction_type = 'REWARD_ACQUISITION_GIFT') ubi ON ubi.user_id = tcr.user_id
)

,kyc as (
            SELECT tcr.user_id,
                  media_source,
                  fecha_kyc,
                  costo_kyc as cac_kyc,
                  coalesce(rer.re_cac,0) as re_cac_kyc,
                  coalesce(ubi.amount,0) as cac_ubi_kyc,
                  costo_kyc + re_cac_kyc + cac_ubi_kyc as cac_kyc_total
        FROM lemoncash_data.temp_cac_kyc tcr
            LEFT JOIN (SELECT * FROM lemoncash_data.CAC_RE WHERE type = 'RE_kyc')rer ON rer.re_users = tcr.user_id
            LEFT JOIN (SELECT user_id, 
                              cast(amount as decimal(32,8))*cast(rate_sale as decimal(32,8)) as amount
                       FROM lemoncash_ar.activities a 
                       INNER JOIN lemoncash_data.exchange_rate_usd er ON er.currency = a.currency and er.fecha = date_trunc('hour', a.createdat) 
                       WHERE transaction_type = 'REWARD_ACQUISITION_GIFT') ubi ON ubi.user_id = tcr.user_id
)

SELECT distinct cast(coalesce(registro.user_id, kyc.user_id) as int) as user_id,
       cast(register_date as timestamp) as register_date,
       coalesce(registro.media_source, kyc.media_source) as media_source,
       cast(cac_registro as decimal(32,8)) as cac_registro,
       cast(re_cac_registro as decimal(32,8)) as re_cac_registro,
       cast(cac_ubi_registro as decimal(32,8)) as cac_ubi_registro,
       cast(cac_registro_total as decimal(32,8)) as cac_registro_total,
       cast(fecha_kyc as timestamp) as kyc_date,
       cast(cac_kyc as decimal(32,8)) as cac_kyc,
       cast(re_cac_kyc as decimal(32,8)) as re_cac_kyc,
       cast(cac_ubi_kyc as decimal(32,8)) as cac_ubi_kyc,
       cast(cac_kyc_total as decimal(32,8)) as cac_kyc_total
       
FROM registro
FULL JOIN kyc ON registro.user_id = kyc.user_id
      )'''

)

insert_CAC_registro >> insert_CAC_RE >> insert_CAC_kyc >> delete_select_table >> select_temp_tables
