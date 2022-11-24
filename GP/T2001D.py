from airflow import DAG, settings, secrets
from airflow.operators.sql import SQLCheckOperator,SQLValueCheckOperator, SQLIntervalCheckOperator,SQLThresholdCheckOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import boto3
import json
from plugins import slack_util

PythonOperator.ui_color = "#ffdef2"
RedshiftSQLOperator.ui_color = "#ddfffc"
S3ToRedshiftOperator.ui_color = "#f2e2ff"
SQLCheckOperator.ui_color ="#ffffe3"
CHANNEL = "#airflow-monitors"
OWNERS= ["U02RTV264B1"]

def get_most_recent_s3_object(bucket_name, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" )
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return latest['Key']
    
def call_lambda_2001():
    hook = AwsLambdaHook(function_name = 'gp_2001',
                         region_name='sa-east-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse')
    response_1 = hook.invoke_lambda(payload=json.dumps({ "RUN": "NORMAL" }))
    print(response_1)

with DAG(
    dag_id="T2001D", 
    start_date= days_ago(1), 
    schedule_interval='10 11 * * *',   
    on_failure_callback =slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['GP']) as dag:

    invoke_lambda_function = PythonOperator(
        task_id='invoke_lambda_function',
        python_callable = call_lambda_2001,
    )
    
print_objects_in_bucket = PythonOperator(
task_id='print_objects_in_bucket',
python_callable=get_most_recent_s3_object,
op_args=['liquidaciones-conciliaciones','Liquidaciones Metabase/T2001D/']
)

create_staging_table = RedshiftSQLOperator(
    task_id='create_staging_table', 
    redshift_conn_id = 'Redshift_Data',
    sql=""" CREATE TABLE LEMONCASH_DATA.STAGING_2001D(
            numero_de_tarjeta   varchar(512) ,
            n_mero_de_cuenta   varchar(512) ,
            numero_de_comercio   varchar(512) ,
            nombre_fantas_a_comercio  varchar(512) ,
            c_digo_de_autorizaci_n  varchar(512) ,
            ica_adquirente   varchar(512) ,
            descripci_n_del_movimiento   varchar(512) ,
            fecha_y_hora_de_la_operaci_n  timestamp ,
            fecha_de_presentaci_n   timestamp ,
            importe_en_moneda_del_movimiento  float ,
            c_digo_de_moneda_original   varchar(512) ,
            importe_en_moneda_original   float ,
            importe_inter_s  float ,
            importe_del_iva   float ,
            importe_total_del_movimiento  float ,
            importe_descuento_financ_otrog   float ,
            importe_descuento_financ_otrog_iva   float ,
            importe_de_compensaci_n  float ,
            signo_importe_compensaci_n   varchar(512) ,
            importe_percep_rg4240   float ,
            signo_percep_rg4240   varchar(512) ,
            moneda_de_compensaci_n   varchar(512) ,
            importe_autorizacion   float ,
            signo_importe_autorizacion   varchar(512) ,
            moneda_de_autorizacion   varchar(512) ,
            importe_autorizacion_convertido  float ,
            signo_importe_autorizacion2   varchar(512) ,
            moneda_autorizacion_convertido   varchar(512) ,
            importe_ipm_de6   float ,
            signo_importe_ipm_de6   varchar(512) ,
            moneda_ipm_de51   varchar(512) ,
            cuenta_externa   varchar(512) ,
            request_id_de_la_autorizacion   varchar(512) ,
            importe_percep_ley_27_541   float ,
            signo_percep_ley_27_541   varchar(512) ,
            iva_servicios_digitales_importe   float ,
            iva_servicios_digitales_signo   varchar(512) ,
            iibb_servicios_digitales_importe  float ,
            iibb_servicios_digitales_signo   varchar(512) ,
            iibb_servicios_digitales_c_digo_de_provincia   varchar(512) ,
            rg_4815_importe   float ,
            rg_4815_signo   varchar(512) ,
            tipo_registro  float ,
            marca   varchar(512) ,
            entidad_emisora   varchar(512) ,
            sucursal   varchar(512) ,
            tipo_de_socio   varchar(512) ,
            grupo_de_cuenta_corriente   varchar(512) ,
            tipo_de_transacci_n   varchar(512) ,
            c_digo_postal_comercio   varchar(512) ,
            codigo_de_movimiento   varchar(512) ,
            tipo_de_plan   varchar(512) ,
            plan_de_cuotas   varchar(512) ,
            n_mero_de_cuota_vigente   varchar(512) ,
            c_digo_de_moneda   varchar(512) ,
            debcred   float ,
            tipo_amortizaci_n   float ,
            tipo_tarjeta  float ,
            tna   float ,
            tea   float ,
            tasa_de_intercambio  float ,
            arancel_emisor  float ,
            signo_iva_arancel   varchar(512) ,
            iva_arancel_emisor   float ,
            signo_iva_arancel2   varchar(512) ,
            motivo_mensaje  varchar(512) ,
            tipo_producto  varchar(512) ,
            fecha_de_cierre_de_comercios   timestamp ,
            plan  varchar(512) ,
            estado  varchar(512) ,
            filler  varchar(512) ,
            autoid  varchar(512) ,
            autocodi  varchar(512) ,
            comprobante  varchar(512) ,
            importe_de_la_cuota    float ,
            importe_inter_s_de_la_cuota   float ,
            import_del_iva_de_la_cuota  float ,
            importe_total_de_la_cuota  float ,
            fecha_de_cierre_de_cuenta_corriente  timestamp ,
            fecha_de_clearing timestamp ,
            fecha_de_diferimiento  timestamp ,
            mcc   varchar(512) ,
            columna_adicional_1   varchar(512) ,
            columna_adicional_2   varchar(512) ,
            columna_adicional_3  varchar(512) 
        )  """)

copy_s3_redshift_staging = S3ToRedshiftOperator(
        
        task_id='copy_s3_redshift_staging',
        s3_bucket='liquidaciones-conciliaciones',
        s3_key ="{{ti.xcom_pull(task_ids='print_objects_in_bucket')}}",
        schema="LEMONCASH_DATA",
        table="STAGING_2001D",
        copy_options = ['IGNOREHEADER 1' ,'removequotes', 'emptyasnull', 'blanksasnull',"DELIMITER ','","region 'sa-east-1'"],
    )


check_wrong_dates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_wrong_dates",
    sql="""WITH stage_table as (select 1 as test,max(fecha_de_presentaci_n) as max_date from LEMONCASH_DATA.STAGING_2001D),
        prod_table as (select 1 as test, max(fecha_de_presentaci_n) as max_date from liquidaciones_conci.T2001D)
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
        select numero_de_tarjeta    ,n_mero_de_cuenta    ,numero_de_comercio    ,nombre_fantas_a_comercio   ,c_digo_de_autorizaci_n   ,ica_adquirente    ,descripci_n_del_movimiento    ,fecha_y_hora_de_la_operaci_n   ,fecha_de_presentaci_n    ,importe_en_moneda_del_movimiento   ,c_digo_de_moneda_original    ,importe_en_moneda_original    ,importe_inter_s   ,importe_del_iva    ,importe_total_del_movimiento   ,importe_descuento_financ_otrog    ,importe_descuento_financ_otrog_iva    ,importe_de_compensaci_n   ,signo_importe_compensaci_n    ,importe_percep_rg4240    ,signo_percep_rg4240    ,moneda_de_compensaci_n    ,importe_autorizacion    ,signo_importe_autorizacion    ,moneda_de_autorizacion    ,importe_autorizacion_convertido   ,signo_importe_autorizacion2    ,moneda_autorizacion_convertido    ,importe_ipm_de6    ,signo_importe_ipm_de6    ,moneda_ipm_de51    ,cuenta_externa    ,request_id_de_la_autorizacion    ,importe_percep_ley_27_541    ,signo_percep_ley_27_541    ,iva_servicios_digitales_importe    ,iva_servicios_digitales_signo    ,iibb_servicios_digitales_importe   ,iibb_servicios_digitales_signo    ,iibb_servicios_digitales_c_digo_de_provincia    ,rg_4815_importe    ,rg_4815_signo    ,tipo_registro   ,marca    ,entidad_emisora    ,sucursal    ,tipo_de_socio    ,grupo_de_cuenta_corriente    ,tipo_de_transacci_n    ,c_digo_postal_comercio    ,codigo_de_movimiento    ,tipo_de_plan    ,plan_de_cuotas    ,n_mero_de_cuota_vigente    ,c_digo_de_moneda    ,debcred    ,tipo_amortizaci_n    ,tipo_tarjeta   ,tna    ,tea    ,tasa_de_intercambio   ,arancel_emisor   ,signo_iva_arancel    ,iva_arancel_emisor    ,signo_iva_arancel2    ,motivo_mensaje   ,tipo_producto   ,fecha_de_cierre_de_comercios    ,plan   ,estado   ,filler   ,autoid   ,autocodi   ,comprobante   ,importe_de_la_cuota     ,importe_inter_s_de_la_cuota    ,import_del_iva_de_la_cuota   ,importe_total_de_la_cuota   ,fecha_de_cierre_de_cuenta_corriente   ,fecha_de_clearing  ,fecha_de_diferimiento   ,mcc    ,columna_adicional_1    ,columna_adicional_2    ,columna_adicional_3   ,COUNT(*)
        from LEMONCASH_DATA.STAGING_2001D
        group by numero_de_tarjeta    ,n_mero_de_cuenta    ,numero_de_comercio    ,nombre_fantas_a_comercio   ,c_digo_de_autorizaci_n   ,ica_adquirente    ,descripci_n_del_movimiento    ,fecha_y_hora_de_la_operaci_n   ,fecha_de_presentaci_n    ,importe_en_moneda_del_movimiento   ,c_digo_de_moneda_original    ,importe_en_moneda_original    ,importe_inter_s   ,importe_del_iva    ,importe_total_del_movimiento   ,importe_descuento_financ_otrog    ,importe_descuento_financ_otrog_iva    ,importe_de_compensaci_n   ,signo_importe_compensaci_n    ,importe_percep_rg4240    ,signo_percep_rg4240    ,moneda_de_compensaci_n    ,importe_autorizacion    ,signo_importe_autorizacion    ,moneda_de_autorizacion    ,importe_autorizacion_convertido   ,signo_importe_autorizacion2    ,moneda_autorizacion_convertido    ,importe_ipm_de6    ,signo_importe_ipm_de6    ,moneda_ipm_de51    ,cuenta_externa    ,request_id_de_la_autorizacion    ,importe_percep_ley_27_541    ,signo_percep_ley_27_541    ,iva_servicios_digitales_importe    ,iva_servicios_digitales_signo    ,iibb_servicios_digitales_importe   ,iibb_servicios_digitales_signo    ,iibb_servicios_digitales_c_digo_de_provincia    ,rg_4815_importe    ,rg_4815_signo    ,tipo_registro   ,marca    ,entidad_emisora    ,sucursal    ,tipo_de_socio    ,grupo_de_cuenta_corriente    ,tipo_de_transacci_n    ,c_digo_postal_comercio    ,codigo_de_movimiento    ,tipo_de_plan    ,plan_de_cuotas    ,n_mero_de_cuota_vigente    ,c_digo_de_moneda    ,debcred    ,tipo_amortizaci_n    ,tipo_tarjeta   ,tna    ,tea    ,tasa_de_intercambio   ,arancel_emisor   ,signo_iva_arancel    ,iva_arancel_emisor    ,signo_iva_arancel2    ,motivo_mensaje   ,tipo_producto   ,fecha_de_cierre_de_comercios    ,plan   ,estado   ,filler   ,autoid   ,autocodi   ,comprobante   ,importe_de_la_cuota     ,importe_inter_s_de_la_cuota    ,import_del_iva_de_la_cuota   ,importe_total_de_la_cuota   ,fecha_de_cierre_de_cuenta_corriente   ,fecha_de_clearing  ,fecha_de_diferimiento   ,mcc    ,columna_adicional_1    ,columna_adicional_2    ,columna_adicional_3   
        HAVING COUNT(*) > 1))
        select case when duplicated_events > 0 then 0 else 1 end as duplicated_check
        from null_rows
    """
    )


insert_into_prod = RedshiftSQLOperator(
    task_id='insert_into_prod', 
    redshift_conn_id = 'Redshift_Data',
    sql= """ INSERT INTO liquidaciones_conci.T2001D (
        SELECT * FROM LEMONCASH_DATA.STAGING_2001D
        )"""
    )

insert_statistic_info = RedshiftSQLOperator(
    task_id='insert_statistic_info', 
    redshift_conn_id = 'Redshift_Data',
        sql="""INSERT INTO LEMONCASH_DATA.MONITOR_DAG_INFO_T2001D  (
    select
    'T2001D',
    count(*) ,
    current_timestamp
    from LEMONCASH_DATA.STAGING_2001D
    );
    """
)


delete_staging_table = RedshiftSQLOperator(
    task_id='delete_staging_table', 
    redshift_conn_id = 'Redshift_Data',
    sql= """ DROP TABLE LEMONCASH_DATA.STAGING_2001D
        """
    )


invoke_lambda_function>>print_objects_in_bucket>>create_staging_table>>copy_s3_redshift_staging>>[check_wrong_dates,check_duplicates]>>insert_into_prod>>insert_statistic_info>>delete_staging_table

