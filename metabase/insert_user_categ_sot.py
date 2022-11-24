import pandas as pd 
import os 
from datetime import timedelta,datetime, date
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from plugins import slack_util
CHANNEL = "#airflow-monitors"
OWNERS= ["U035P7MR3UZ"]

with DAG(
    dag_id="insert_user_categ_sot", 
    start_date=datetime(2022, 11, 1), 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    schedule_interval= '30 7 * * *',
    tags = ['categoria','table','update','daily']
    ) as dag:

    create_staging_table_actividades = RedshiftSQLOperator(
        task_id = 'create_staging_actividades',
        sql = """ create table lemoncash_data.staging_user_categ_actividades as 
            (SELECT 
                distinct user_id,
                date_trunc('month',createdat) as mes 
            FROM lemoncash_ar.activities 
            WHERE transaction_type in 
                ('LEMON_CARD_PAYMENT',
                'CRYPTO_SWAP',
                'CRYPTO_PURCHASE',
                'CRYPTO_SALE',
                'VIRTUAL_DEPOSIT',
                'VIRTUAL_WITHDRAWAL',
                'CASH_IN_CRYPTO',
                'WALLET_TO_EXTERNAL_CRYPTO_WALLET',
                'WALLET_TO_WALLET')
            )""")

    create_staging_table_last_login = RedshiftSQLOperator(
        task_id = 'create_staging_last_login',
        sql = """ create table lemoncash_data.staging_user_categ_last_login as
            (
            SELECT 
                distinct user_id,
                date_trunc('month','1970-01-01 00:00:00 GMT'::timestamp +  login_time / 1000 * interval '1 second') as mes 
            FROM lemoncash_dynamo.hevo_app_accesses_pro
            WHERE  '1970-01-01 00:00:00 GMT'::timestamp +  login_time / 1000 * interval '1 second' < current_timestamp
            and user_id = 74707
            
            )""")
        
    create_staging_table_saldos = RedshiftSQLOperator(
        task_id = 'create_staging_saldos',
        sql = """ create table lemoncash_data.staging_user_categ_saldos as
            (
            select 
                sum(usd_amount) suma, 
                user_id,
                (year||'-'||month||'-'||day)::date as dia 
            from lemoncash_data.balances 
            group by 2,3
            ) """)

    create_staging_table_promedio_mensual = RedshiftSQLOperator(
        task_id = 'create_staging_promedio_mensual_saldo',
        sql = """ create table lemoncash_data.staging_user_categ_promedio_mensual as 
            (
            select  
                avg(suma) as promedio, 
                user_id,
                date_trunc('month',dia) as mes 
            from lemoncash_data.staging_user_categ_saldos 
            group by 2,3
        )""")

    create_staging_table_suscripcion = RedshiftSQLOperator(
        task_id = 'create_staging_suscripcion',
        sql = """ create table lemoncash_data.staging_user_categ_suscripcion as
            (
                select 
                    distinct userid,
                    enabled 
                from lemoncash_ar.interestfundsubscriptions a 
                inner join lemoncash_ar.wallets b on a.wallet_id = b.id 
                )""")
    
    create_staging_table_meses = RedshiftSQLOperator(
        task_id = 'create_staging_meses',
        sql = """ create table lemoncash_data.staging_user_categ_meses as
            (
                    SELECT  *, 
                        case when first_tx is not null then all_months end as month
                        FROM 
                            (SELECT distinct date_trunc('month',createdat) as all_months
                            FROM lemoncash_ar.activities a 
                            )a
        
                            LEFT JOIN 
                            
                            (SELECT 
                                min(date_trunc('month',createdat)) as first_tx,
                                user_id
                            FROM lemoncash_ar.activities a 
                            GROUP BY 2
                            )c
                            ON all_months >= first_tx
                        order by 3,1 
                    )""")
        
    create_staging_table_ctu_tx = RedshiftSQLOperator(
        task_id = 'create_staging_user_categ_ctu_tx',
        sql = """ create table lemoncash_data.staging_user_categ_ctu_tx as
            (select distinct user_id, date_trunc('month', createdat) mes 
                        from lemoncash_ar.activities 
                        where transaction_type in 
                            ('VIRTUAL_DEPOSIT',
                            'VIRTUAL_WITHDRAWAL',
                            'CASH_IN_CRYPTO',
                            'WALLET_TO_EXTERNAL_CRYPTO_WALLET',
                            'WALLET_TO_WALLET')
            )""")
        
    create_staging_table_rtu_tx = RedshiftSQLOperator(
        task_id = 'create_staging_user_categ_rtu_tx',
        sql = """ create table lemoncash_data.staging_user_categ_rtu_tx as
            (select distinct user_id, date_trunc('month', createdat) mes 
                        from lemoncash_ar.activities 
                        where transaction_type in 
                            ('LEMON_CARD_PAYMENT',
                            'CRYPTO_SALE',
                            'CRYPTO_PURCHASE',
                            'CRYPTO_SWAP')
            )""")

    delete_staging_table_general = RedshiftSQLOperator(
        task_id = 'drop_staging_general',
        sql = """drop table lemoncash_data.staging_user_Categ_general""",
        trigger_rule="all_done"
    )

    delete_staging_table_2 = RedshiftSQLOperator(
        task_id = 'drop_staging_2',
        sql = """drop table lemoncash_data.staging_user_categ_2""",
        trigger_rule="all_done"
    )

    delete_staging_table_actividades = RedshiftSQLOperator(
        task_id = 'drop_staging_actividades',
        sql = """drop table lemoncash_data.staging_user_categ_actividades""",
        trigger_rule="all_done"
    )

    delete_staging_table_last_login = RedshiftSQLOperator(
        task_id = 'drop_staging_last_login',
        sql = """drop table lemoncash_data.staging_user_categ_last_login""",
        trigger_rule="all_done"
    )

    delete_staging_table_saldos = RedshiftSQLOperator(
        task_id = 'drop_staging_saldos',
        sql = """drop table lemoncash_data.staging_user_categ_saldos""",
        trigger_rule="all_done"
    )

    delete_staging_table_promedio_mensual = RedshiftSQLOperator(
        task_id = 'drop_staging_promedio_mensual',
        sql = """drop table lemoncash_data.staging_user_categ_promedio_mensual""",
        trigger_rule="all_done"
    )

    delete_staging_table_suscripcion = RedshiftSQLOperator(
        task_id = 'drop_staging_suscripcion',
        sql = """drop table lemoncash_data.staging_user_categ_suscripcion""",
        trigger_rule="all_done"
    )

    delete_staging_table_meses = RedshiftSQLOperator(
        task_id = 'drop_staging_meses',
        sql = """drop table lemoncash_data.staging_user_categ_meses""",
        trigger_rule="all_done"
    )

    delete_staging_table_ctu_tx = RedshiftSQLOperator(
        task_id = 'drop_staging_ctu_tx',
        sql = """drop table lemoncash_data.staging_user_categ_ctu_tx""",
        trigger_rule="all_done"
    )

    delete_staging_table_rtu_tx = RedshiftSQLOperator(
        task_id = 'drop_staging_rtu_tx',
        sql = """drop table lemoncash_data.staging_user_categ_rtu_tx""",
        trigger_rule="all_done"
    )

    create_staging_table_general = RedshiftSQLOperator(
        task_id='create_staging_table_general', 
        sql=""" CREATE TABLE lemoncash_data.STAGING_user_categ_general as (
        select 
            distinct mes.month as mes,
            mes.user_id,
            case when state = 'ENABLED' then 1 else 0 end as kyc,
            case when a.user_id is not null then 1 else 0 end as actividades,
            case when ll.user_id is not null then 1 else 0 end as login,
            case when pm.user_id is not null then 1 else 0 end as saldo,
            case when pm.promedio >= 25 then 1 else 0 end as saldo_mayor_25,
            case when sus.userid is not null then 1 else 0 end as earn,
            case when ctutx.user_id is not null then 1 else 0 end as actividades_ctu,
            case when rtutx.user_id is not null then 1 else 0 end as actividades_rtu
            
        from lemoncash_data.staging_user_categ_meses mes     
        left join lemoncash_ar.accounts acc on mes.user_id = acc.owner_id 
        left join lemoncash_data.staging_user_categ_actividades a on mes.month = a.mes and mes.user_id = a.user_id
        left join lemoncash_data.staging_user_categ_last_login ll on mes.month = ll.mes and mes.user_id = ll.user_id
        left join lemoncash_data.staging_user_categ_promedio_mensual pm on mes.month = pm.mes and mes.user_id = pm.user_id 
        left join lemoncash_data.staging_user_categ_suscripcion sus on mes.user_id = sus.userid
        left join lemoncash_data.staging_user_categ_ctu_tx ctutx on mes.month = ctutx.mes and mes.user_id = ctutx.user_id
        left join lemoncash_data.staging_user_categ_rtu_tx rtutx on mes.month = rtutx.mes and mes.user_id = rtutx.user_id
        order by 1 desc 
        ) """)

    create_staging_table_2 = RedshiftSQLOperator(
        task_id = 'create_staging_2',
        sql = """ create table lemoncash_data.staging_user_categ_2 as 
            (select *
            from lemoncash_data.staging_user_categ_general
            where date_trunc('month',mes) = (select date_trunc('month',current_date) as mes)
            )"""
    )

    delete_rows = RedshiftSQLOperator(
        task_id = 'delete_current_month_rows',
        sql = """ delete from mtus.tabla
                where mes = date_trunc('month',current_date) """
    )

    insert_rows = RedshiftSQLOperator(
        task_id = 'insert_current_month_rows',
        sql = """ insert into mtus.tabla(
            select * from lemoncash_data.staging_user_categ_2
        )"""
    )

    delete_rows_mt = RedshiftSQLOperator(
        task_id = 'delete_current_month_rows_mt',
        sql = """ delete from lemoncash_data.user_categ
                where mes = date_trunc('month',current_date) """
    )

    insert_rows_mt = RedshiftSQLOperator(
        task_id = 'insert_current_month_rows_mt',
        sql = """ insert into lemoncash_data.user_categ(
            select 
                mes, user_id,
                case when login = 1 and saldo_mayor_25 = 0 and saldo = 1 and actividades = 0 then 1 else 0 end as hiu_holder, -- tiene saldo, entra en la app, pero < 25 USD
                case when login = 0 and saldo_mayor_25 =0 and saldo = 1 and actividades = 0 then 1 else 0 end as hiu_inactive, -- tiene saldo, no entra en la app, saldo < 25 USD 
                case when saldo_mayor_25 = 1 and earn = 1 and actividades = 0 then 1 else 0 end as hiu_rtu, -- saldo > 25 y tiene earn 
                case when saldo_mayor_25 = 1 and earn = 0 and actividades = 0 then 1 else 0 end as hiu_ctu, -- saldo > 25 y no tiene earn 
                actividades_rtu as rtu,
                case when actividades_ctu = 1 and actividades_rtu = 0 and saldo = 1 then 1 else 0 end as ctu,
                '|||' as col,
                case when (actividades_rtu = 0 and actividades_ctu = 1 and saldo = 1) or (actividades = 0 and earn = 0 and saldo = 1) then 1 else 0 end as ctu_old,
                case when saldo = 1 and actividades = 0 and earn = 1 then 1 else 0 end as hiu_old,
                '|||' as col1,
                rtu + ctu_old + hiu_old as mtu_old, 
                rtu + hiu_inactive + hiu_holder + hiu_ctu + hiu_rtu + ctu as mtu_new 

            from mtus.tabla 
            where mes = date_trunc('month',current_date)
            order by 1 desc
        )"""
    )

# truncate_master_table = RedshiftSQLOperator(
#     task_id = 'truncate_master_table_user_categ',
#     sql = """ truncate table lemoncash_data.user_categ"""
# )

# insert_into_master_table = RedshiftSQLOperator(
#     task_id = 'insert_into_master_table_user_categ',
#     sql = """ insert into lemoncash_data.user_categ (
#         select 
#             mes, user_id,
#             case when login = 1 and saldo_mayor_25 = 0 and saldo = 1 and actividades = 0 then 1 else 0 end as hiu_holder, -- tiene saldo, entra en la app, pero < 25 USD
#             case when login = 0 and saldo_mayor_25 =0 and saldo = 1 and actividades = 0 then 1 else 0 end as hiu_inactive, -- tiene saldo, no entra en la app, saldo < 25 USD 
#             case when saldo_mayor_25 = 1 and earn = 1 and actividades = 0 then 1 else 0 end as hiu_rtu, -- saldo > 25 y tiene earn 
#             case when saldo_mayor_25 = 1 and earn = 0 and actividades = 0 then 1 else 0 end as hiu_ctu, -- saldo > 25 y no tiene earn 
#             actividades_rtu as rtu,
#             case when actividades_ctu = 1 and actividades_rtu = 0 and saldo = 1 then 1 else 0 end as ctu,
#             '|||' as col,
#             case when (actividades_rtu = 0 and actividades_ctu = 1 and saldo = 1) or (actividades = 0 and earn = 0 and saldo = 1) then 1 else 0 end as ctu_old,
#             case when saldo = 1 and actividades = 0 and earn = 1 then 1 else 0 end as hiu_old,
#             '|||' as col1,
#             rtu + ctu_old + hiu_old as mtu_old, 
#             rtu + hiu_inactive + hiu_holder + hiu_ctu + hiu_rtu + ctu as mtu_new 

#         from mtus.tabla 
#         where mes >= date_trunc('month','2021-01-01'::date)
#         order by 1 desc
#     )"""
# )

    check_duplicates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_duplicates",
    sql="""WITH null_rows as (
        select count(*) as duplicated_events from (
        select mes, user_id, COUNT(*)
        from lemoncash_data.staging_user_categ_general
        group by 1,2
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
                from lemoncash_data.staging_user_categ_general
                where mes is null or user_id is null or saldo is null or actividades is null
                )
            )
        select case when null_events > 0 then 0 else 1 end as null_check
        from null_rows
    """
    )

    [create_staging_table_actividades,create_staging_table_last_login,create_staging_table_saldos,create_staging_table_suscripcion,create_staging_table_meses,create_staging_table_ctu_tx,create_staging_table_rtu_tx]>>create_staging_table_promedio_mensual>>create_staging_table_general>>create_staging_table_2>>delete_rows>>insert_rows>>[check_duplicates,check_nulls]>>delete_rows_mt>>insert_rows_mt>>[delete_staging_table_general,delete_staging_table_actividades,delete_staging_table_last_login,delete_staging_table_saldos,delete_staging_table_promedio_mensual,delete_staging_table_suscripcion,delete_staging_table_meses,delete_staging_table_ctu_tx,delete_staging_table_rtu_tx,delete_staging_table_2]