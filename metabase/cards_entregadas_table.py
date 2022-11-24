from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="insert_into_entregadas",
    start_date=datetime(2022, 9, 22),
    tags=['cards', 'insert', 'table', 'data'],
    schedule_interval='30 2 * * *'
    ) as dag:

  truncate_table_cards_entregadas = RedshiftSQLOperator(
      task_id='truncate_table_cards_entregadas',
      sql = 'truncate table lemoncash_data.cards_entregadas;'
  )
  
  insert_table_cards_entregadas = RedshiftSQLOperator(
      task_id='insert_table_cards_entregadas',
      sql = """
      insert into lemoncash_data.cards_entregadas (select a.user_id, a.dni, c.numero_tracking, a.card_id, a.state, a.nro_tarjeta, a.codigo_postal, a.region, a.fecha_solicitud, c.fecha_entrega, a.fecha_activacion, DATEDIFF(days, a.fecha_solicitud, c.fecha_entrega) AS demora, a.activada
from
(select uc.id as user_id, 
    uc.identity_document_value as dni,
    c.id as card_id,
    c.state,
    row_number() over (partition by dni order by fecha_solicitud ASC) as nro_tarjeta,
    cast(s.zip_code as decimal(36,8)) as codigo_postal,
    r.region,
    c.created_at - interval '3 hour' as fecha_solicitud, 
    c.updated_at - interval '3 hour' as fecha_activacion,
    case when (c.state = 'ACTIVE') then 1 else 0 end as activada

from lemoncash_ar.cards c 
inner join lemoncash_ar.cardaccounts ca on c.card_account_id = ca.id
inner join lemoncash_ar.accounts a on a.id = ca.user_account_id
inner join lemoncash_ar.shippingaddresses s on s.card_account_id = ca.id
inner join lemoncash_ar.userscash uc on uc.id = a.owner_id
inner join lemoncash_ar.users u on u.id = uc.id
inner join gsheets.regiones_andreani r on s.zip_code = r.codigo_postal
where operation_country = 'ARG'
order by 5 desc
) a

inner join 
(select numero_tracking,
       dni,
       fecha_entrega,
       row_number() over (partition by dni order by fecha_entrega ASC) as ocurrencias,
       region
from(
select hist.numero_tracking, dni, max(fecha) as fecha_entrega, r.region
from bi_trackingandreani.tracking_history hist
inner join bi_trackingandreani.dniynumerotracking id on hist.numero_tracking = id.numero_tracking
inner join gsheets.regiones_andreani r on id.codigo_postal = r.codigo_postal

where estado = 'Entregado'
group by 1,2,4
order by 1) 
order by 4 desc ) c  on a.dni = c.dni and a.nro_tarjeta = c.ocurrencias

-- where demora between 0 and 15
  --  and date_part('dow' , fecha_entrega) <> 0
    
order by 1   );
    """
    )

  truncate_table_cards_entregadas >> insert_table_cards_entregadas