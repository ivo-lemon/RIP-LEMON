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
    dag_id="insert_metricas_finance", 
    start_date=days_ago(1), 
    schedule_interval='0 5 * * *', 
    dagrun_timeout=timedelta(minutes=60),
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla','metricas','finance']) as dag:

    truncate_finance = RedshiftSQLOperator(
        task_id='truncate_finance', 
        sql="TRUNCATE lemoncash_data.metricas_finance;"
    )
    

    insert_metricas_finance = RedshiftSQLOperator(
        task_id='insert_metricas_finance', 
        sql=""" INSERT INTO lemoncash_data.metricas_finance
(select distinct
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
    transacting_cards_tvp_USD,
    transacting_card_tvp_ARS,
    transacting_card_tvp_TOTAL,
    transacting_cards_tx_efectivas,
    transacting_cards_tx_authorizations,
    auc_fiat_en_usd,
    auc_crypto_en_usd,
    fau_rtu,
    interchange_fee_usd,
    interchange_fee_ars,
    total_fee_amount_usd,
    users_saldo_mayor_25_usd
    
from 
-- NEW USERS 
    (
    select 
        count(distinct owner_id) as new_users, 
        date_trunc('month',created_at) as mes,
        'new users' as tipo 
    from lemoncash_ar.accounts a inner join lemoncash_ar.users b on a.owner_id = b.id
        where operation_country = 'ARG'
    group by 2
    ) a 

FULL JOIN 
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
        where operation_country = 'ARG'
        group by 2
        )
    ) b on a.mes = b.mes 
    
FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) c on a.mes = c.mes 
    
FULL JOIN 
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) d on a.mes = d.mes 

FULL JOIN 
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
    and updated_by_id is null

    GROUP BY 2
    
    order by 2
    ) e on a.mes = e.mes 
    
FULL JOIN 

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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) f on a.mes = f.mes 
    
FULL JOIN 

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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) g on a.mes = g.mes 

FULL JOIN     
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) h on a.mes = h.mes 
    
FULL JOIN     
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) h2 on a.mes = h2.mes 
    
FULL JOIN 
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) i on a.mes = i.mes 
    
FULL JOIN 
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) j on a.mes = j.mes 

FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) k on a.mes = k.mes 
    
FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) l on a.mes = l.mes 
    
FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) l2 on a.mes = l2.mes 
    
FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) m on a.mes = m.mes 
    
FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) n on a.mes = n.mes 
    
FULL JOIN 
-- NEW CARDS
    (
    SELECT 
        count(distinct u.id) AS new_cards,
        date_trunc('month',  u.created_at)  as mes,
        'new cards' as tipo 

    FROM  lemoncash_ar.cards u 
    GROUP BY 2
    ) o on a.mes = o.mes 

FULL JOIN 
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
    GROUP BY 2
        )
    ) p on a.mes = p.mes 

FULL JOIN 
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) q on a.mes = q.mes 

FULL JOIN 
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'

    GROUP BY 2
    
    order by 2
    ) r on a.mes = r.mes 
FULL JOIN 
-- TVP CRYPTO TRANSACTING USERS 

    (
    select sum(volumen_hora) as crypto_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'crypto transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_PURCHASE' OR
            transaction_type = 'CRYPTO_SWAP' OR
            transaction_type = 'CRYPTO_SALE' OR
            transaction_type = 'AUTOSWAP'
            )
    group by 2,3 
    ) s on a.mes = s.mes 
    
FULL JOIN 
-- # TX CRYPTO TRANSACTING USERS

    (
    select sum(cantidad_hora) as crypto_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'crypto transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_PURCHASE' OR
            transaction_type = 'CRYPTO_SWAP' OR
            transaction_type = 'CRYPTO_SALE' OR
            transaction_type = 'AUTOSWAP'
            )
    group by 2,3 
    ) t on a.mes = t.mes 

FULL JOIN 
-- TVP PURCHASE TRANSACTING USERS 

    (
    select sum(volumen_hora) as purchase_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'purchase transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_PURCHASE'
            )
    group by 2,3 
    ) u on a.mes = u.mes 
    
FULL JOIN 
-- # TX PURCHASE TRANSACTING USERS

    (
    select sum(cantidad_hora) as purchase_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'purchase transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_PURCHASE')
    group by 2,3 
    ) v on a.mes = v.mes

FULL JOIN 
-- TVP SALE TRANSACTING USERS 

    (
    select sum(volumen_hora) as sale_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'sale transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_SALE')
    group by 2,3 
    ) w on a.mes = w.mes 
    
FULL JOIN 
-- # TX SALE TRANSACTING USERS

    (
    select sum(cantidad_hora) as sale_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'sale transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_SALE'  
            )
    group by 2,3 
    ) x on a.mes = x.mes
    
FULL JOIN 
-- TVP AUTOSWAP TRANSACTING USERS 

    (
    select sum(volumen_hora) as autoswap_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'autoswap transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'AUTOSWAP')
    group by 2,3 
    ) w2 on a.mes = w2.mes 

    
FULL JOIN 
-- # TX AUTOSWAP TRANSACTING USERS

    (
    select sum(cantidad_hora) as autoswap_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'autoswap transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'AUTOSWAP'  
            )
    group by 2,3 
    ) x2 on a.mes = x2.mes


FULL JOIN 
-- TVP SWAP TRANSACTING USERS 

    (
    select sum(volumen_hora) as swap_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'swap transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_SWAP')
    group by 2,3 
    ) y on a.mes = y.mes 
    
FULL JOIN 
-- # TX SWAP TRANSACTING USERS

    (
    select sum(cantidad_hora) as swap_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'swap transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'CRYPTO_SWAP'  
            )
    group by 2,3 
    ) z on a.mes = z.mes

FULL JOIN 
-- TVP EARN TRANSACTING USERS 

    (
    select sum(volumen_hora) as earn_transacting_users_tvp,
            date_trunc('month', hora) as mes,
            'earn transacting users tvp' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'INTEREST_EARNING')
    group by 2,3 
    ) aa on a.mes = aa.mes 
    
FULL JOIN 
-- # TX EARN TRANSACTING USERS

    (
    select sum(cantidad_hora) as earn_transacting_users_tx,
            date_trunc('month', hora) as mes,
            'earn transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'INTEREST_EARNING'  
            )
    group by 2,3 
    ) bb on a.mes = bb.mes
    
FULL JOIN 
-- TVP CARD TRANSACTING USERS EN USD y ARS

    (select 
    date_trunc('month', (fecha_de_presentaci_n::date) - '1 day'::interval) as mes, 
    sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 and signo_importe_compensaci_n = '+' then cast(importe_de_compensaci_n as decimal(36,8))
            when cast(moneda_de_compensaci_n as decimal(36,8)) = 32 and signo_importe_compensaci_n = '-' then cast(importe_de_compensaci_n as decimal(36,8)) * (-1)
            else 0 end) as transacting_card_tvp_ARS, 
    sum(case when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 and signo_importe_compensaci_n = '+' then cast(importe_de_compensaci_n as decimal(36,8))
            when cast(moneda_de_compensaci_n as decimal(36,8)) = 840 and signo_importe_compensaci_n = '-' then cast(importe_de_compensaci_n as decimal(36,8)) * (-1)
            else 0 end) as transacting_cards_tvp_USD
    from liquidaciones_conci.t2001d
    group by 1
    order by 1 desc
    ) cc on cc.mes = a.mes


FULL JOIN
--TVP CARD TOTAL

    (select date_trunc('month',hora) as mes, sum(volumen_hora) as transacting_card_tvp_TOTAL
    from lemoncash_data.tvp_finance
    where transaction_type = 'LEMON_CARD_PAYMENT'
    group by 1) cc2 on cc2.mes = a.mes
    
FULL JOIN 
-- # TX CARD TRANSACTING USERS EFECTIVAS
    (
    select sum(cantidad_hora) as transacting_cards_tx_efectivas,
            date_trunc('month', hora) as mes,
            'card transacting users #tx' as tipo 
    from lemoncash_data.tvp_finance
    where (transaction_type = 'LEMON_CARD_PAYMENT'  
            )
    group by 2,3 
    ) dd on a.mes = dd.mes
    
FULL JOIN 
-- # TX CARD TRANSACTING USERS AUTHORIZATIONS
    (
    select date_trunc('month', created_at) as mes, count(distinct id) as transacting_cards_tx_authorizations
    from lemoncash_ar.lemoncardpaymentauthorizationrequests
    group by 1 
    order by 1 desc
    ) dd2 on a.mes = dd2.mes
    
FULL JOIN
-- AUC CRYPTO
    (select date_trunc('month',dia) as mes, auc_crypto_en_usd
    from
        (select sum(balance_usd) auc_crypto_en_usd, categoria, dia
        from 
            (select *, case when asset = 'MONEY' then 'PESOS' else 'CRYPTO' end as categoria, cast(fecha as date) as dia
            from snapshots.auc
            where 1=1
            -- cast(fecha as date) >=  '2022-01-01'
            and categoria = 'CRYPTO'
            ) a 
            inner join 
            (select distinct date_trunc('month',createdat) as mes, cast(max(createdat) as date) as max_dia from lemoncash_ar.activities group by 1) b
            on b.max_dia = a.dia
        group by 2,3)
    order by 1 desc
    ) ee on ee.mes = a.mes

FULL JOIN
-- AUC FIAT
    (select date_trunc('month',dia) as mes, auc_fiat_en_usd
    from
        (select sum(balance_usd) auc_fiat_en_usd, categoria, dia
        from 
            (select *, case when asset = 'MONEY' then 'PESOS' else 'CRYPTO' end as categoria, cast(fecha as date) as dia
            from snapshots.auc
            where 1=1 
            ---and cast(fecha as date) >=  '2022-01-01'
            and categoria = 'PESOS'
            ) a 
            inner join 
            (select distinct date_trunc('month',createdat) as mes, cast(max(createdat) as date) as max_dia from lemoncash_ar.activities group by 1) b
            on b.max_dia = a.dia
        group by 2,3)
    order by 1 desc
    ) ff on ff.mes = a.mes
    
FULL JOIN
-- KYC ACUMULADOS
        (select mes, sum(cant_kyc) over (order by mes rows unbounded preceding) as kyc_acumulados
        from
            (select date_trunc('month',updated_at) as mes, count(distinct user_id) as cant_kyc
            from lemoncash_ar.kycs
            where state = 'VALIDATED'
            group by 1)
        order by 1 desc
        ) gg on gg.mes = a.mes

FULL JOIN
--KYC mensuales iniciados
        (select date_trunc('month',created_at) as mes, count(distinct user_id) as kyc_solicitados
        from lemoncash_ar.kycs
        group by 1
        order by 1 desc) hh on hh.mes = a.mes

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
    
    WHERE 1=1 --primera_tx >= '2022-01-01'
    and operation_country = 'ARG'
    GROUP BY 2
    ) ii on ii.mes = a.mes
    
full join
-- interchange fees de cards, los de ARS y los de USD + la suma de ambos mostrada en USD
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
        c as (select date_trunc('day',cast(fecha as date) ) as fecha, billete_compra, billete_venta, divisa_compra, divisa_venta
            from gsheets.tc_oficial_bna_base_tc_hist_rico
            where fecha is not null
            and fecha != 'Fuente: BNA'
            and billete_venta is not null)
        SELECT  
                sum(case when a.currency = 'USD' then fee_amount end) as interchange_fee_usd,
                sum(case when a.currency = 'MONEY' then fee_amount end) as interchange_fee_ars,
                SUM(case when a.currency = 'USD' then (fee_amount*coalesce(tc_factura,billete_venta))/ccl
                        when a.currency = 'MONEY' then fee_amount/ccl end) as total_fee_amount_usd,
                concept_code,
                date_trunc('month',dia) as mes
        FROM a
        full JOIN b 
            on date_trunc('month', a.dia) = b.mes
        full join c
            on date_trunc('day', c.fecha) = a.dia
        where 1=1 
        --and mes >= '2022-01-01'
        group by 4,5
        order by 5 desc) jj on jj.mes = a.mes

full join
    (select date_trunc('month',mes) as mes, sum(hiu_rtu) as users_saldo_mayor_25_usd
    from lemoncash_data.user_categ
    group by 1
    ) kk on kk.mes = a.mes

where a.mes is not null
    and a.mes >= '2021-11-01'
order by 1 desc)   

""")

    
    truncate_finance >> insert_metricas_finance

