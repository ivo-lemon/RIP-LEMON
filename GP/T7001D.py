from airflow import DAG, settings, secrets
from airflow.operators.sql import SQLCheckOperator,SQLValueCheckOperator, SQLIntervalCheckOperator,SQLThresholdCheckOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
import boto3
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

def call_lambda_7001():
    hook = AwsLambdaHook(function_name = 'gp_7001',
                         region_name='sa-east-1',
                         log_type='None', qualifier='$LATEST',
                         invocation_type='RequestResponse')
    response_1 = hook.invoke_lambda(payload='null')
    print(response_1)

with DAG(
    dag_id="T7001D", 
    start_date= days_ago(1), 
    schedule_interval='20 11 * * *',  
    on_failure_callback =slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['GP']) as dag:

    invoke_lambda_function = PythonOperator(
        task_id='invoke_lambda_function',
        python_callable = call_lambda_7001,
    )

print_objects_in_bucket = PythonOperator(
task_id='print_objects_in_bucket',
python_callable=get_most_recent_s3_object,
op_args=['liquidaciones-conciliaciones','Liquidaciones Metabase/T7001D/']
)

create_staging_table = RedshiftSQLOperator(
    task_id='create_staging_table', 
    redshift_conn_id = 'Redshift_Data',
    sql=""" CREATE TABLE LEMONCASH_DATA.STAGING_7001D(
        nro_de_cuenta    varchar(512) ,
        n_de_tarjeta    varchar(512) ,
        fecha_procesamiento_hora_min_seg   timestamp ,
        importe   float ,
        automone    varchar(512) ,
        plan    varchar(512) ,
        c_digo_de_autorizaci_n    varchar(512) ,
        autoforzada   int ,
        autoreverflag    int ,
        autoautodebi   int ,
        n_de_comercio    varchar(512) ,
        nombre_del_comercio    varchar(512) ,
        estado    varchar(512) ,
        rechazo    varchar(512) ,
        ica   varchar(512) ,
        mcc     int ,
        automoneoriiso    varchar(512) ,
        importe_original_autoriz    float ,
        importe_autoriz_convertido    float ,
        signo_importe_autoriz_convertido   varchar(512) ,
        importe_rg4240    float ,
        signo_importe_rg4240   varchar(512) ,
        importe_iibb_rg4240   float ,
        signo_importe_iibb_rg4240    varchar(512) ,
        provincia_iibb_rg4240   varchar(512) ,
        importe_impuesto_ley_pais    float ,
        signo_importe_impuesto_ley_pais   varchar(512) ,
        fecha_estado_hora_min_seg    timestamp ,
        importe_rg4815   float ,
        signo_importe_rg4815    varchar(512) ,
        cuenta_externa    varchar(512) ,
        request_id    varchar(512) ,
        tipo_de_registro   int ,
        adicional_n    varchar(512) ,
        tipo_de_moneda    varchar(512) ,
        cuotas    varchar(512) ,
        relacionada    varchar(512) ,
        origen    varchar(512) ,
        tcc    varchar(512) ,
        c_digo_regla_de_fraude    varchar(512) ,
        descripci_n_regla_de_fraude    varchar(512) ,
        modeo_de_entrada    varchar(512) ,
        terminal_pos    varchar(512) ,
        estado1    varchar(512) ,
        standin    varchar(512) ,
        filler    varchar(512) ,
        usuario    varchar(512) ,
        total_de_importe_cargos   float ,
        signo_importe_cargos    varchar(512) ,
        total_de_importe_iva_cargos    float ,
        signo_importe_iva_cargos    varchar(512) ,
        total_de_importe_cargos_convertido    float ,
        signo_cargos_convertido   varchar(512) ,
        total_de_importe_iva_cargos_convertido    float ,
        signo_iva_cargos_convertido   varchar(512) ,
        concepto_cargo_1   varchar(512) ,
        importe_cargo_1    float ,
        signo_importe_cargo_1   varchar(512) ,
        importe_iva_cargo_1    float ,
        concepto_cargo_2   varchar(512) ,
        importe_cargo_2     float ,
        signo_importe_cargo_2   varchar(512) ,
        importe_iva_cargo_2     float ,
        concepto_cargo_3    varchar(512) ,
        importe_cargo_3   float ,
        signo_importe_cargo_3    varchar(512) ,
        importe_iva_cargo_3    float ,
        columna_adicional_1    int ,
        columna_adicional_2   int ,
        columna_adicional_3   int
        );  """)

copy_s3_redshift_staging = S3ToRedshiftOperator(
        
        task_id='copy_s3_redshift_staging',
        s3_bucket='liquidaciones-conciliaciones',
        s3_key ="{{ti.xcom_pull(task_ids='print_objects_in_bucket')}}",
        schema="LEMONCASH_DATA",
        table="STAGING_7001D",
        copy_options = ['IGNOREHEADER 1' ,'removequotes', 'emptyasnull', 'blanksasnull',"DELIMITER ','","region 'sa-east-1'"],
    )


check_wrong_dates = SQLCheckOperator(
    conn_id="Redshift_Data",
    task_id="check_wrong_dates",
    sql="""WITH stage_table as (select 1 as test,max(fecha_procesamiento_hora_min_seg) as max_date from LEMONCASH_DATA.STAGING_7001D),
        prod_table as (select 1 as test, max(fecha_procesamiento_hora_min_seg) as max_date from liquidaciones_conci.T7001D)
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
        select nro_de_cuenta     ,n_de_tarjeta     ,fecha_procesamiento_hora_min_seg    ,importe    ,automone     ,plan     ,c_digo_de_autorizaci_n     ,autoforzada    ,autoreverflag     ,autoautodebi    ,n_de_comercio     ,nombre_del_comercio     ,estado     ,rechazo     ,ica    ,mcc      ,automoneoriiso     ,importe_original_autoriz     ,importe_autoriz_convertido     ,signo_importe_autoriz_convertido    ,importe_rg4240     ,signo_importe_rg4240    ,importe_iibb_rg4240    ,signo_importe_iibb_rg4240     ,provincia_iibb_rg4240    ,importe_impuesto_ley_pais     ,signo_importe_impuesto_ley_pais    ,fecha_estado_hora_min_seg     ,importe_rg4815    ,signo_importe_rg4815     ,cuenta_externa     ,request_id     ,tipo_de_registro    ,adicional_n     ,tipo_de_moneda     ,cuotas     ,relacionada     ,origen     ,tcc     ,c_digo_regla_de_fraude     ,descripci_n_regla_de_fraude     ,modeo_de_entrada     ,terminal_pos     ,estado1     ,standin     ,filler     ,usuario     ,total_de_importe_cargos    ,signo_importe_cargos     ,total_de_importe_iva_cargos     ,signo_importe_iva_cargos     ,total_de_importe_cargos_convertido     ,signo_cargos_convertido    ,total_de_importe_iva_cargos_convertido     ,signo_iva_cargos_convertido    ,concepto_cargo_1    ,importe_cargo_1     ,signo_importe_cargo_1    ,importe_iva_cargo_1     ,concepto_cargo_2    ,importe_cargo_2      ,signo_importe_cargo_2    ,importe_iva_cargo_2      ,concepto_cargo_3     ,importe_cargo_3    ,signo_importe_cargo_3     ,importe_iva_cargo_3     ,columna_adicional_1     ,columna_adicional_2    ,columna_adicional_3   ,COUNT(*)
        from LEMONCASH_DATA.STAGING_7001D
        group by nro_de_cuenta     ,n_de_tarjeta     ,fecha_procesamiento_hora_min_seg    ,importe    ,automone     ,plan     ,c_digo_de_autorizaci_n     ,autoforzada    ,autoreverflag     ,autoautodebi    ,n_de_comercio     ,nombre_del_comercio     ,estado     ,rechazo     ,ica    ,mcc      ,automoneoriiso     ,importe_original_autoriz     ,importe_autoriz_convertido     ,signo_importe_autoriz_convertido    ,importe_rg4240     ,signo_importe_rg4240    ,importe_iibb_rg4240    ,signo_importe_iibb_rg4240     ,provincia_iibb_rg4240    ,importe_impuesto_ley_pais     ,signo_importe_impuesto_ley_pais    ,fecha_estado_hora_min_seg     ,importe_rg4815    ,signo_importe_rg4815     ,cuenta_externa     ,request_id     ,tipo_de_registro    ,adicional_n     ,tipo_de_moneda     ,cuotas     ,relacionada     ,origen     ,tcc     ,c_digo_regla_de_fraude     ,descripci_n_regla_de_fraude     ,modeo_de_entrada     ,terminal_pos     ,estado1     ,standin     ,filler     ,usuario     ,total_de_importe_cargos    ,signo_importe_cargos     ,total_de_importe_iva_cargos     ,signo_importe_iva_cargos     ,total_de_importe_cargos_convertido     ,signo_cargos_convertido    ,total_de_importe_iva_cargos_convertido     ,signo_iva_cargos_convertido    ,concepto_cargo_1    ,importe_cargo_1     ,signo_importe_cargo_1    ,importe_iva_cargo_1     ,concepto_cargo_2    ,importe_cargo_2      ,signo_importe_cargo_2    ,importe_iva_cargo_2      ,concepto_cargo_3     ,importe_cargo_3    ,signo_importe_cargo_3     ,importe_iva_cargo_3     ,columna_adicional_1     ,columna_adicional_2    ,columna_adicional_3   
        HAVING COUNT(*) > 1))
        select case when duplicated_events > 0 then 0 else 1 end as duplicated_check
        from null_rows
    """
    )


insert_into_prod = RedshiftSQLOperator(
    task_id='insert_into_prod', 
    redshift_conn_id = 'Redshift_Data',
    sql= """ INSERT INTO liquidaciones_conci.T7001D (
        SELECT * FROM LEMONCASH_DATA.STAGING_7001D
        )"""
    )

insert_statistic_info = RedshiftSQLOperator(
    task_id='insert_statistic_info', 
    redshift_conn_id = 'Redshift_Data',
    sql="""INSERT INTO LEMONCASH_DATA.MONITOR_DAG_INFO_T7001D  (
    select
    'T7001D',
    count(*) ,
    current_timestamp
    from LEMONCASH_DATA.STAGING_7001D
    );
    """
) 


delete_staging_table = RedshiftSQLOperator(
    task_id='delete_staging_table', 
    redshift_conn_id = 'Redshift_Data',
    sql= """ DROP TABLE LEMONCASH_DATA.STAGING_7001D
        """
    )


invoke_lambda_function>>print_objects_in_bucket>>create_staging_table>>copy_s3_redshift_staging>>[check_wrong_dates,check_duplicates]>>insert_into_prod>>insert_statistic_info>>delete_staging_table

