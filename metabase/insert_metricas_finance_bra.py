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
    dag_id="insert_metricas_finance_brasil", 
    start_date=days_ago(1), 
    schedule_interval='30 5 * * *', 
    dagrun_timeout=timedelta(minutes=60),
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla','metricas','finance']) as dag:

    truncate_finance_brasil = RedshiftSQLOperator(
        task_id='truncate_finance_brasil', 
        sql="TRUNCATE lemoncash_data.metricas_finance_brasil;"
    )
    

    insert_metricas_finance_brasil = RedshiftSQLOperator(
        task_id='insert_metricas_finance_brasil', 
        sql=""" INSERT INTO lemoncash_data.metricas_finance_brasil 
(
select distinct
    a.mes,
    new_users,
    usuarios_acumulados,
    kyc_acumulados,
    kyc_solicitados,
    transacting_user,
    new_transacting_users,
    new_transacting_crypto_users,
    eop_crypto_transacting_user,
    new_purchase_transacting_users,
    new_sale_transacting_users,
    new_autoswap_transacting_users,
    new_swap_transacting_users,
    new_earn_transacting_users,
    eop_purchase_transacting_user,
    eop_sale_transacting_user,
    eop_autoswap_transacting_user,
    eop_swap_transacting_user,
    eop_earn_transacting_user,
    new_cards,
    cards_acumuladas,
    eop_transacting_card,
    new_transacting_cards,
    crypto_transacting_users_tvp,
    crypto_transacting_users_tx,
    purchase_transacting_users_tvp,
    purchase_transacting_users_tx,
    sale_transacting_users_tvp,
    sale_transacting_users_tx,
    autoswap_transacting_users_tvp,
    autoswap_transacting_users_tx,
    swap_transacting_users_tvp,
    swap_transacting_users_tx,
    earn_transacting_users_tvp,
    earn_transacting_users_tx,
    auc_brl_en_usd,
    auc_crypto_en_usd,
    fau_rtu

from
-- NEW USERS
    (
    select
        count(distinct owner_id) as new_users,
        date_trunc('month',created_at) as mes,
        'new users' as tipo
    from lemoncash_ar.accounts a inner join lemoncash_ar.users b on a.owner_id = b.id
        where operation_country = 'BRA'
        and b.id not in (18144,17626,1343730,1343764)
    group by 2
    ) a

INNER JOIN
-- ACUMM USERS
    (
    select
        sum(cantidad) over (order by month rows unbounded preceding) as usuarios_acumulados,
        month as mes ,
        'usuarios acumulados' as tipo
    from
        (
        select
            count(distinct a.id) as cantidad,
            date_trunc('month',a.created_at) as month
        from lemoncash_ar.accounts a inner join lemoncash_ar.users b on a.owner_id = b.id
        where b.operation_country = 'BRA'
        and b.id not in (18144,17626,1343730,1343764)
        group by 2
        )
    ) b on a.mes = b.mes

left JOIN
-- TRANSACTING USERS
    (
    SELECT
        count(distinct user_id) as transacting_user,
        date_trunc('month', a.createdat) as mes,
        'transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','LEMON_CARD_PAYMENT')--,'INTEREST_EARNING')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) c on a.mes = c.mes

left JOIN
-- NEW TRANSACTING USERS
    (
    SELECT
       count(distinct act.user_id) AS new_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_PURCHASE' OR
             transaction_type = 'CRYPTO_SWAP' OR
             transaction_type = 'CRYPTO_SALE' OR
             transaction_type = 'LEMON_CARD_PAYMENT' --OR
             --transaction_type = 'INTEREST_EARNING'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'

    GROUP BY 2

    order by 2
    ) d on a.mes = d.mes

left JOIN
-- NEW TRANSACTING CRYPTO
    (
    SELECT
       count(distinct act.user_id) AS new_transacting_crypto_users,
       date_trunc('month',  u.createdat)  as mes,
        'new transacting crypto users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_PURCHASE' OR
             transaction_type = 'CRYPTO_SWAP' OR
             transaction_type = 'CRYPTO_SALE'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'

    GROUP BY 2

    order by 2
    ) e on a.mes = e.mes

left JOIN

-- EOP CRYPTO TRANSACTING
    (
    SELECT
        count(distinct user_id) as eop_crypto_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop crypto transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) f on a.mes = f.mes

left JOIN

-- NEW PURCHASE TRANSACTING

    (
    SELECT
       count(distinct act.user_id) AS new_purchase_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new purchase transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_PURCHASE'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'
    AND state = 'DONE'
    AND updated_by_id is null

    GROUP BY 2

    order by 2
    ) g on a.mes = g.mes

left JOIN
-- NEW SALE TRANSACTING

    (
    SELECT
       count(distinct act.user_id) AS new_sale_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new sale transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_SALE'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'
    AND state = 'DONE'
    AND updated_by_id is null

    GROUP BY 2

    order by 2
    ) h on a.mes = h.mes

left JOIN
-- NEW AUTOSWAP TRANSACTING

    (
    SELECT
       count(distinct act.user_id) AS new_autoswap_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new autoswap transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and ((transaction_type = 'CRYPTO_SALE') and visible = 0)

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'
    AND state = 'DONE'
    AND updated_by_id is null

    GROUP BY 2

    order by 2
    ) h2 on a.mes = h2.mes

left JOIN
-- NEW SWAP TRANSACTING
    (
    SELECT
       count(distinct act.user_id) AS new_swap_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new swap transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_SWAP'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'
    AND state = 'DONE'
    AND updated_by_id is null

    GROUP BY 2

    order by 2
    ) i on a.mes = i.mes

left JOIN
-- NEW EARN USER
    (
    SELECT
       count(distinct act.user_id) AS new_earn_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new earn transacting users' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'INTEREST_EARNING'
             )

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'
    AND state = 'DONE'
    AND updated_by_id is null

    GROUP BY 2

    order by 2
    ) j on a.mes = j.mes

left JOIN
-- EOP PURCHASE
    (
    SELECT
        count(distinct user_id) as eop_purchase_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop purchase transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('CRYPTO_PURCHASE')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) k on a.mes = k.mes

left JOIN
-- EOP SALE

    (
    SELECT
        count(distinct user_id) as eop_sale_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop sale transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('CRYPTO_SALE')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) l on a.mes = l.mes

left JOIN
-- EOP AUTOSWAP

    (
    SELECT
        count(distinct user_id) as eop_autoswap_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop autoswap transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where (transaction_type in ('CRYPTO_SALE') and visible = 0)
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) l2 on a.mes = l2.mes

left JOIN
-- EOP SWAP
    (
    SELECT
        count(distinct user_id) as eop_swap_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop swap transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('CRYPTO_SWAP')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) m on a.mes = m.mes

left JOIN
-- EOP INTEREST EARNING
    (
    SELECT
        count(distinct user_id) as eop_earn_transacting_user,
        date_trunc('month', a.createdat) as mes ,
        'eop earn transacting users' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('INTEREST_EARNING')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)
    group by 2
    ) n on a.mes = n.mes

left JOIN
-- NEW CARDS
    (
     SELECT
       count(distinct u.id) AS new_cards,
       date_trunc('month',  u.created_at)  as mes,
        'new cards' as tipo

     FROM  lemoncash_ar.cards u
     inner join lemoncash_ar.cardaccounts c on u.card_account_id = c.id
     inner join lemoncash_ar.accounts a on a.id = c.user_account_id
     INNER JOIN lemoncash_ar.users us on us.id = a.owner_id

     where us.id not in (18144,17626,1343730,1343764)
     and operation_country = 'BRA'

     GROUP BY 2
   ) o on a.mes = o.mes

left JOIN
-- ACUM CARDS
    (
    select
        sum(new_cards) over (order by mes rows unbounded preceding) as cards_acumuladas,
        mes ,
        'cards acumuladas' as tipo
    from
        (
     SELECT
       count(distinct u.id) AS new_cards,
       date_trunc('month',  u.created_at)  as mes

     FROM  lemoncash_ar.cards u
     inner join lemoncash_ar.cardaccounts c on u.card_account_id = c.id
     inner join lemoncash_ar.accounts a on a.id = c.user_account_id
     INNER JOIN lemoncash_ar.users us on us.id = a.owner_id

     where us.id not in (18144,17626,1343730,1343764)
     and us.operation_country = 'BRA'

     GROUP BY 2
        )
    ) p on a.mes = p.mes

left JOIN
-- EOP CARDS
    (
    SELECT
        count(distinct user_id) as eop_transacting_card,
        date_trunc('month', a.createdat) as mes ,
        'eop transacting cards' as tipo

    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
    where transaction_type in ('LEMON_CARD_PAYMENT')
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'BRA'
        AND user_id not in (18144,17626,1343730,1343764)

    group by 2
    ) q on a.mes = q.mes

left JOIN
-- NEW CARDS

    (
    SELECT
       count(distinct act.user_id) AS new_transacting_cards,
       date_trunc('month',  u.createdat)  as mes,
        'new transacting cards' as tipo

    FROM  lemoncash_ar.users u
    inner join  lemoncash_ar.activities act on act.user_id = u.id

    where
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'LEMON_CARD_PAYMENT')

    and user_id not in (18144,17626,1343730,1343764)
    and operation_country = 'BRA'

    GROUP BY 2

    order by 2
    ) r on a.mes = r.mes

LEFT JOIN
-- KYC ACUMULADOS
        (select mes, sum(cant_kyc) over (order by mes rows unbounded preceding) as kyc_acumulados
        from
            (select date_trunc('month',updated_at) as mes, count(distinct user_id) as cant_kyc
            from lemoncash_ar.kycs k
            inner join lemoncash_ar.users u on k.user_id = u.id

            where state = 'VALIDATED'
            and u.operation_country = 'BRA'
            and u.id not in (18144,17626,1343730,1343764)

            group by 1)
        order by 1 desc
        )gg on gg.mes = a.mes

LEFT JOIN
--KYC mensuales iniciados
        (select date_trunc('month',created_at) as mes, count(distinct user_id) as kyc_solicitados
        from lemoncash_ar.kycs k
        inner join lemoncash_ar.users u on k.user_id = u.id

        where u.operation_country = 'BRA'
        and u.id not in (18144,17626,1343730,1343764)

        group by 1
        order by 1 desc) hh on hh.mes = a.mes
        

FULL JOIN 
-- TVP CRYPTO TRANSACTING USERS 

    (
    select sum(volumen) as crypto_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'crypto transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_PURCHASE' OR
             transaction_type = 'CRYPTO_SWAP' OR
             transaction_type = 'CRYPTO_SALE' OR
             transaction_type = 'AUTOSWAP'
             )
     group by 2,3 
    ) ii on ii.mes = a.mes

FULL JOIN 
-- Tx CRYPTO TRANSACTING USERS 

    (
    select sum(cantidad_mes) as crypto_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'crypto transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_PURCHASE' OR
             transaction_type = 'CRYPTO_SWAP' OR
             transaction_type = 'CRYPTO_SALE' OR
             transaction_type = 'AUTOSWAP'
             )
     group by 2,3 
    ) jj on jj.mes = a.mes

FULL JOIN 
-- TVP PURCHASE TRANSACTING USERS 

    (
    select sum(volumen) as purchase_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'purchase transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_PURCHASE'
             )
     group by 2,3 
    ) kk on kk.mes = a.mes

FULL JOIN 
-- Tx PURCHASE TRANSACTING USERS 

    (
    select sum(cantidad_mes) as purchase_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'purchase transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_PURCHASE'
             )
     group by 2,3 
    ) ll on ll.mes = a.mes

FULL JOIN 
-- TVP SALE TRANSACTING USERS 

    (
    select sum(volumen) as sale_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'sale transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_SALE'
             )
     group by 2,3 
    ) mm on mm.mes = a.mes

FULL JOIN 
-- Tx SALE TRANSACTING USERS 

    (
    select sum(cantidad_mes) as sale_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'sale transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_SALE'
             )
     group by 2,3 
    ) nn on nn.mes = a.mes

FULL JOIN 
-- TVP AUTOSWAP TRANSACTING USERS 

    (
    select sum(volumen) as autoswap_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'autoswap transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'AUTOSWAP'
             )
     group by 2,3 
    ) oo on oo.mes = a.mes

FULL JOIN 
-- Tx autoswap TRANSACTING USERS 

    (
    select sum(cantidad_mes) as autoswap_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'autoswap transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'AUTOSWAP'
             )
     group by 2,3 
    ) pp on pp.mes = a.mes

FULL JOIN 
-- TVP swap TRANSACTING USERS 

    (
    select sum(volumen) as swap_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'swap transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_SWAP'
             )
     group by 2,3 
    ) qq on qq.mes = a.mes

FULL JOIN 
-- Tx swap TRANSACTING USERS 

    (
    select sum(cantidad_mes) as swap_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'swap transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'CRYPTO_SWAP'
             )
     group by 2,3 
    ) rr on rr.mes = a.mes

FULL JOIN 
-- TVP earn TRANSACTING USERS 

    (
    select sum(volumen) as earn_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'earn transacting users tvp' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'INTEREST_EARNING'
             )
     group by 2,3 
    ) ss on ss.mes = a.mes

FULL JOIN 
-- Tx earn TRANSACTING USERS 

    (
    select sum(cantidad_mes) as EARN_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'EARN transacting users tx' as tipo 
     from lemoncash_data.daily_tvp_bra
     where (transaction_type = 'INTEREST_EARNING'
             )
     group by 2,3 
    ) tt on tt.mes = a.mes

FULL JOIN
-- AUC BRL
    (select date_trunc('month',coalesce(a.dia,b.dia)) as mes, coalesce(a.auc_brl_en_usd,b.auc_brl_en_usd) as auc_brl_en_usd
    from
        (select sum(balance_usd) auc_brl_en_usd, categoria, date_trunc('day',cast(fecha as date)) as dia
        from 
            (select *, case when asset = 'BRL' then 'BRL' else 'CRYPTO' end as categoria
            from lemoncash_data.auc
            where cast(fecha as date) >=  '2022-01-01'
            and country = 'BRA'
            and categoria = 'BRL'
            )
        where dia in (select distinct date_trunc('month',createdat)-'1 day'::interval as mes from lemoncash_ar.activities)
        group by 2,3) a
        
        full join 
        
        (select sum(balance_usd) auc_brl_en_usd, categoria, date_trunc('day',cast(fecha as date)) as dia
        from 
            (select *, case when asset = 'BRL' then 'BRL' else 'CRYPTO' end as categoria
            from lemoncash_data.auc
            where cast(fecha as date) >=  '2022-01-01'
            and categoria = 'BRL'
            and country = 'BRA'
            )
        where dia = (select max(date_trunc('day',fecha::date)) from snapshots.auc where asset = 'BRL')
        group by 2,3) b on a.dia = b.dia and a.categoria = b.categoria
    
    
    order by 1 desc) uu on uu.mes = a.mes

FULL JOIN
-- AUC CRYPTO
    (select date_trunc('month',coalesce(a.dia,b.dia)) as mes, coalesce(a.auc_brl_en_usd,b.auc_brl_en_usd) as auc_crypto_en_usd
    from
        (select sum(balance_usd) auc_brl_en_usd, categoria, date_trunc('day',cast(fecha as date)) as dia
        from 
            (select *, case when asset = 'BRL' then 'BRL' else 'CRYPTO' end as categoria
            from lemoncash_data.auc
            where cast(fecha as date) >=  '2022-01-01'
            and country = 'BRA'
            and categoria = 'CRYPTO'
            )
        where dia in (select distinct date_trunc('month',createdat)-'1 day'::interval as mes from lemoncash_ar.activities)
        group by 2,3) a
        
        full join 
        
        (select sum(balance_usd) auc_brl_en_usd, categoria, date_trunc('day',cast(fecha as date)) as dia
        from 
            (select *, case when asset = 'BRL' then 'BRL' else 'CRYPTO' end as categoria
            from lemoncash_data.auc
            where cast(fecha as date) >=  '2022-01-01'
            and categoria = 'CRYPTO'
            and country = 'BRA'
            )
        where dia = (select max(date_trunc('day',fecha::date)) from snapshots.auc where asset = 'BRL')
        group by 2,3) b on a.dia = b.dia and a.categoria = b.categoria
    
    
    order by 1 desc) uu2 on uu2.mes = a.mes

FULL JOIN
--FAU_RTU
    (
    SELECT count(distinct user_id) as fau_rtu,
           date_trunc('month', primera_tx) as mes
    FROM ( select min(mes) as primera_tx,
                  user_id
        from (select a.createdat as mes, user_id 
                  from lemoncash_ar.activities a
        where 1=1
        AND state = 'DONE'
        AND updated_by_id is null
        and transaction_type in ('LEMON_CARD_PAYMENT','CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP')
        AND user_id not in (18144,17626)
        and user_id not in (1343730, 1343764) --nuevos KUKU y FINDI
        )
            group by 2) a
            
    inner join lemoncash_ar.users u on u.id = a.user_id
    
    WHERE primera_tx >= '2022-01-01'
    and operation_country = 'BRA'
    GROUP BY 2
    ) vv on vv.mes = a.mes
    

ORDER BY 1 DESC)
     """)

    
    truncate_finance_brasil >> insert_metricas_finance_brasil

