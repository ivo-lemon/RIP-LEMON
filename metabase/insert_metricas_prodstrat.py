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
    dag_id="insert_metricas_prod_strat", 
    start_date=days_ago(1), 
    schedule_interval='0 6 * * *', 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla']) as dag:

    truncate_metricas_prodstrat = RedshiftSQLOperator(
        task_id='truncate_metricas_prodstrat', 
        sql="TRUNCATE lemoncash_data.metricas_prodstrat;"
    )
    
    

    insert_metricas_prodstrat= RedshiftSQLOperator(
        task_id='insert_metricas_prodstrat', 
        sql=""" INSERT INTO lemoncash_data.metricas_prodstrat

(select 
    b.mes, 
    usuarios_acumulados, 
    new_users,
    new_kyc,
    transacting_user, 
    new_transacting_users, 
    transacting_user_totalTx,
    new_transacting_users_totalTx,
    new_cards,
    cards_acumuladas,
    cards_solicitadas,
    cards_activadas,
    eop_transacting_card,
    new_transacting_cards,
    new_transacting_compra_venta_users, 
    new_purchase_transacting_users,
    new_sale_transacting_users,
    new_autoswap_transacting_users,
    new_total_sale_transacting_users,
    new_swap_transacting_users,
    new_earn_transacting_users,
    eop_compra_venta_transacting_user,
    eop_purchase_transacting_user,
    eop_sale_transacting_user,
    eop_autoswap_transacting_user,
    eop_total_sale_users,
    eop_swap_transacting_user,
    eop_earn_transacting_user,
    eop_users_trf_in,
    eop_users_trf_out,
    eop_users_cash_in,
    eop_users_cash_out,
    tx_cards,
    tx_purchase,
    tx_sale,
    tx_autoswap,
    tx_swap,
    tx_cvu_in,
    tx_cvu_out,
    tx_cash_in,
    tx_cash_out,
    vol_card_BRUTO,
    vol_card_NETO,
    vol_compra,
    vol_sale,
    vol_autoswap,
    vol_swap,
    vol_cvu_in,
    vol_cvu_out,
    vol_cash_in,
    vol_cash_out,
    crypto_user,
    CRYPTO_JUST_EARN_USER,
    CRYPTO_CARD_USER,
    CARD_USER,
    new_crypto_user,
    new_CRYPTO_JUST_EARN_USER,
    new_CRYPTO_CARD_USER,
    new_CARD_USER
    
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
    
left JOIN
--NEW KYC
    (select date_trunc('month', k.updated_at) as mes, count(k.id) as new_kyc, 'new_kyc' as tipo
    from lemoncash_ar.kycs k 
    inner join lemoncash_ar.users u on k.user_id = u.id
    where k.state = 'VALIDATED'
        and mes >= '2022-01-01'
    group by 1
    order by 1) kyc on kyc.mes = a.mes


full JOIN 
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
    
left JOIN 
-- TRANSACTING USERS  
    (
    SELECT 
        count(distinct user_id) as transacting_user, 
        date_trunc('month', a.createdat) as mes,
        'transacting users' as tipo  
        
    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
    where transaction_type in ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','LEMON_CARD_PAYMENT','AUTOSWAP') 
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
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
             transaction_type = 'AUTOSWAP' OR
             transaction_type = 'LEMON_CARD_PAYMENT')
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) d on a.mes = d.mes 


left JOIN 
-- TRANSACTING USERS TODAS LAS TX
    (
    SELECT 
        count(distinct user_id) as transacting_user_totalTx, 
        date_trunc('month', a.createdat) as mes,
        'transacting users' as tipo  
        
    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
    where state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) cc on a.mes = cc.mes 
    
left JOIN 
-- NEW TRANSACTING USERS TODAS LAS TX
    (
    SELECT 
       count(distinct act.user_id) AS new_transacting_users_totalTx,
       date_trunc('month',  u.createdat)  as mes,
        'new transacting users' as tipo 
       
    FROM  lemoncash_ar.users u 
    inner join  lemoncash_ar.activities act on act.user_id = u.id
    
    where 
        act.createdat <= (u.createdat + interval '30 day')
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) dd on a.mes = dd.mes 


left JOIN 
-- NEW TRANSACTING COMPRA Y VENTA CRYPTO
    (
    SELECT 
       count(distinct act.user_id) AS new_transacting_compra_venta_users,
       date_trunc('month',  u.createdat)  as mes,
        'new transacting compra venta users' as tipo 
       
    FROM  lemoncash_ar.users u 
    inner join  lemoncash_ar.activities act on act.user_id = u.id
    
    where act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_PURCHASE' OR
             --transaction_type = 'CRYPTO_SWAP' OR
             transaction_type = 'CRYPTO_SALE' 
             )
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) e on a.mes = e.mes 
    
left JOIN 

-- EOP CRYPTO TRANSACTING 
    (
    SELECT 
        count(distinct user_id) as eop_compra_venta_transacting_user, 
        date_trunc('month', a.createdat) as mes ,
        'eop compra venta transacting users' as tipo 
        
    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
    where transaction_type in ('CRYPTO_PURCHASE','CRYPTO_SALE')--,'CRYPTO_SWAP') 
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    order by 2 desc
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
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
        and (transaction_type = 'CRYPTO_SALE' and visible = 1
             )
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) h on a.mes = h.mes 
    
left JOIN 
-- NEW AUTOSWAP
    (
    SELECT 
       count(distinct act.user_id) AS new_autoswap_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new autoswap transacting users' as tipo 
       
    FROM  lemoncash_ar.users u 
    inner join  lemoncash_ar.activities act on act.user_id = u.id
    
    where 
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_SALE' and visible = 0 
             )
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) h2 on h2.mes = a.mes
    
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
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
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
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
    where (transaction_type in ('CRYPTO_SALE')  and visible  = 1)
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) l on a.mes = l.mes 
    
left JOIN 
-- EOP AUTOSWAP

    (
    SELECT 
        count(distinct user_id) as eop_autoswap_transacting_user, 
        date_trunc('month', a.createdat) as mes ,
        'eop autoswwap transacting users' as tipo 
        
    from lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
    where (transaction_type in ('CRYPTO_SALE') and visible = 0) 
        AND state = 'DONE'
        AND updated_by_id is null
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
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
        AND operation_country = 'ARG'
        AND user_id not in (18144,17626,1343730, 1343764)
    group by 2 
    ) q on a.mes = q.mes 

left JOIN 
-- NEW CARDS 

    (SELECT date_trunc('month',  c.created_at - interval '3 hour')  as mes, 
           count(distinct act.user_id) AS new_transacting_cards,
           'new_transacting_cards' as tipo 
           
    FROM  lemoncash_ar.cards c
    inner join lemoncash_ar.cardaccounts ca on c.card_account_id = ca.id
    inner join  lemoncash_ar.activities act on act.account_id = ca.user_account_id
    inner join  lemoncash_ar.users u on act.user_id = u.id
    
    where  transaction_type = 'LEMON_CARD_PAYMENT'
        and act.createdat <= (c.created_at + interval '30 day')
        and operation_country = 'ARG'
        and user_id not in (18144,17626,1343730, 1343764)
     
    GROUP BY 1
    order by 1) r on a.mes = r.mes 
    
left JOIN
-- TARJETAS SOLICITADAS
    (SELECT date_trunc('month', created_at) as mes, COUNT(distinct id ) as cards_solicitadas, 'tarjetas_solicitadas' as tipo
    FROM lemoncash_ar.cards
    where mes >= '2022-01-01'
    group by 1
    order by 1) cards_sol on cards_sol.mes = a.mes

left JOIN
--TARJETAS ACTIVADAS
    (SELECT date_trunc('month',  c.updated_at)  as mes,
           count(*) AS cards_activadas, 'cards_activadas' as tipo
           
    FROM lemoncash_ar.cards c
    inner join lemoncash_ar.cardaccounts a on c.card_account_id = a.id
    inner join lemoncash_ar.accounts b on b.id = a.user_account_id
    left join lemoncash_ar.users u  on b.owner_id = u.id
    
    where c.state = 'ACTIVE'
        and mes >= '2022-01-01'
    GROUP BY 1
    order by 1) cards_act on cards_act.mes = a.mes

left JOIN
--cant users trf in
    (select date_trunc('month', createdat) as mes, count(distinct user_id) as eop_users_trf_in, 'users_trf_in' as tipo
    from lemoncash_ar.activities 
    
    where transaction_type = 'VIRTUAL_DEPOSIT'
        and operation_type = 'CREDIT'
        and currency = 'MONEY'
        and state = 'DONE'
        and user_id not in (18144,17626,1343730, 1343764)
        and updated_by_id is null
        and createdat >= '2022-01-01'
        
    group by 1
    ORDER BY 1 ) trfin on trfin.mes = a.mes

left JOIN
--cant users trf out
    (select date_trunc('month', createdat) as mes, count(distinct user_id) as eop_users_trf_out, 'users_trf_out' as tipo
    from lemoncash_ar.activities 
    
    where transaction_type = 'VIRTUAL_WITHDRAWAL'
        and operation_type = 'DEBIT'
        and currency = 'MONEY'
        and state = 'DONE'
        and user_id not in (18144,17626,1343730, 1343764)
        and updated_by_id is null
        and createdat >= '2022-01-01'
        
    group by 1
    ORDER BY 1 ) trfout on trfout.mes = a.mes

left JOIN
--users cash in crypto
    (select date_trunc('month', createdat) as mes, count(distinct user_id) as eop_users_cash_in, 'users_cash_in' as tipo
    from lemoncash_ar.activities 
    
    where transaction_type = 'CASH_IN_CRYPTO'
        and operation_type = 'CREDIT'
        and state = 'DONE'
        and user_id not in (18144,17626,1343730, 1343764)
        and updated_by_id is null
        and createdat >= '2022-01-01'
        
    group by 1
    ORDER BY 1 ) cashin on cashin.mes = a.mes

left JOIN
--users cash out crypto
    (select date_trunc('month', createdat) as mes, count(distinct user_id) as eop_users_cash_out, 'users_cash_out' as tipo
    from lemoncash_ar.activities 
    
    where transaction_type = 'WALLET_TO_EXTERNAL_CRYPTO_WALLET'
        and operation_type = 'DEBIT'
        and state = 'DONE'
        and user_id not in (18144,17626,1343730, 1343764)
        and updated_by_id is null
        and createdat >= '2022-01-01'
        
    group by 1
    ORDER BY 1 ) cashout on cashout.mes = a.mes

left JOIN
--Tx cards
    (SELECT date_trunc('month', fecha_de_presentaci_n::date) as mes, count(*) as tx_cards
    from liquidaciones_conci.t2001d
    GROUP BY 1
    order by 1) txcards on txcards.mes = a.mes

left JOIN
--Tx Compra crypto
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_purchase, 'tx_purchase' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'CRYPTO_PURCHASE'
    group by 1
    ORDER BY 1) txcompra on txcompra.mes = a.mes

left JOIN
--Tx Sale crypto
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_sale, 'tx_sale' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'CRYPTO_SALE'
    group by 1
    ORDER BY 1) txsale on txsale.mes = a.mes

left JOIN
--Tx AUTOSWAP
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_autoswap, 'tx_autoswap' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'AUTOSWAP'
    group by 1
    ORDER BY 1) txautoswap on txautoswap.mes = a.mes

left JOIN
--Tx SWAP
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_swap, 'tx_swap' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'CRYPTO_SWAP'
    group by 1
    ORDER BY 1) txswap on txswap.mes = a.mes

left join 
--tx cvu in
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_cvu_in, 'tx_cvu_in' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'VIRTUAL_DEPOSIT'
    group by 1
    ORDER BY 1) cvuin on cvuin.mes = a.mes

left join 
--tx cvu out
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_cvu_out, 'tx_cvu_out' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'VIRTUAL_WITHDRAWAL'
    group by 1
    ORDER BY 1) cvuout on cvuout.mes = a.mes

left join
--cash in
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_cash_in, 'tx_cash_in' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'CASH_IN_CRYPTO'
    group by 1
    ORDER BY 1) txcashin on txcashin.mes = a.mes

left join
--cash out
    (select date_trunc('month', hora) as mes, sum(cantidad_mes) as tx_cash_out, 'tx_cash_out' as tipo
    from lemoncash_data.daily_tvp
    where mes >= '2022-01-01'
    and transaction_type = 'WALLET_TO_EXTERNAL_CRYPTO_WALLET'
    group by 1
    ORDER BY 1) txcashout on txcashout.mes = a.mes

left join
-- vol cards metabase (BRUTO)
    (select date_trunc('month',a.hora) as mes, sum(a.volumen) as vol_card_BRUTO
    from lemoncash_data.daily_tvp a
    where transaction_type = 'LEMON_CARD_PAYMENT'
    group by 1
    order by 1)  volcardbruto on volcardbruto.mes = a.mes

left join
-- vol cards metabase (NETO)
    (select date_trunc('month',a.hora) as mes,sum(a.volumen_hora) as vol_card_NETO
    from lemoncash_data.tvp_finance a
    where a.transaction_type = 'LEMON_CARD_PAYMENT'
    group by 1
    order by 1)  volcardneto on volcardneto.mes = a.mes

left join
-- vol compra crypto
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_compra
    from lemoncash_data.daily_tvp
    where transaction_type = 'CRYPTO_PURCHASE'
    group by 1
    order by 1) volcompra on volcompra.mes = a.mes

left join
-- vol sale
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_sale
    from lemoncash_data.daily_tvp
    where transaction_type = 'CRYPTO_SALE'
    group by 1
    order by 1) volsale on volsale.mes = a.mes

left join
-- vol autoswap
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_autoswap
    from lemoncash_data.daily_tvp
    where transaction_type = 'AUTOSWAP'
    group by 1
    order by 1) volautoswap on volautoswap.mes = a.mes

left join
-- vol swap estan incluidos los ARBS
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_swap
    from lemoncash_data.daily_tvp
    where transaction_type = 'CRYPTO_SWAP'
    group by 1
    order by 1) volswap on volswap.mes = a.mes

left join
-- vol cvu in
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_cvu_in
    from lemoncash_data.daily_tvp
    where transaction_type = 'VIRTUAL_DEPOSIT'
    group by 1
    order by 1) volcvuin on volcvuin.mes = a.mes

left join
-- vol cvu out
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_cvu_out
    from lemoncash_data.daily_tvp
    where transaction_type = 'VIRTUAL_WITHDRAWAL'
    group by 1
    order by 1) volcvuout on volcvuout.mes = a.mes

left join
-- vol cash in
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_cash_in
    from lemoncash_data.daily_tvp
    where transaction_type = 'CASH_IN_CRYPTO'
    group by 1
    order by 1) volcashin on volcashin.mes = a.mes

left join
-- vol cash out
    (select date_trunc('month',hora) as mes, sum(volumen) as vol_cash_out
    from lemoncash_data.daily_tvp
    where transaction_type = 'WALLET_TO_EXTERNAL_CRYPTO_WALLET'
    group by 1
    order by 1) volcashout on volcashout.mes = a.mes

left join
--query sebi para users cards, crypto y earn pero excluyendose entre si (que tenga card y no crypto ni earn y asi con todas)
    (select * from    (       SELECT 
                 a.mes as mes
                
                ,COUNT(DISTINCT CASE WHEN (b.user_id IS NOT NULL OR bb.user_id IS NOT NULL
                                       OR  c.user_id IS NOT NULL 
                                       OR  d.user_id IS NOT NULL)
                                       AND f.user_id IS     NULL THEN a.user_id ELSE NULL END) AS CRYPTO_USER
        
                ,COUNT(DISTINCT CASE WHEN  b.user_id IS     NULL 
                                      AND bb.user_id IS     NULL
                                       AND c.user_id IS     NULL 
                                       AND d.user_id IS     NULL
                                       AND f.user_id IS NOT NULL THEN a.user_id ELSE NULL END) AS CARD_USER
        
                ,COUNT(DISTINCT CASE WHEN  b.user_id IS     NULL  
                                      AND bb.user_id IS     NULL
                                       AND c.user_id IS     NULL 
                                       AND d.user_id IS     NULL 
                                       AND e.user_id IS NOT NULL
                                       AND f.user_id IS     NULL THEN a.user_id ELSE NULL END) AS CRYPTO_JUST_EARN_USER
                
                ,COUNT(DISTINCT CASE WHEN (b.user_id IS NOT NULL OR bb.user_id IS NOT NULL
                                       OR  c.user_id IS NOT NULL 
                                       OR  d.user_id IS NOT NULL)
                                       AND f.user_id IS NOT NULL THEN a.user_id ELSE NULL END) AS CRYPTO_CARD_USER
                    
        
                FROM (  SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes
                        ,a.user_id  
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id                  
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type IN ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','INTEREST_EARNING','LEMON_CARD_PAYMENT')) a
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'CRYPTO_SALE'
                            AND a.visible = 1
                            ) b ON a.mes = b.mes AND a.user_id = b.user_id
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'CRYPTO_SALE'
                            AND COALESCE(a.visible,0) = 0
                            ) bb ON a.mes = bb.mes AND a.user_id = bb.user_id
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'CRYPTO_PURCHASE'
                            ) c ON a.mes = c.mes AND a.user_id = c.user_id
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'CRYPTO_SWAP'
                            ) d ON a.mes = d.mes AND a.user_id = d.user_id
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'INTEREST_EARNING'
                            ) e ON a.mes = e.mes AND a.user_id = e.user_id
                LEFT JOIN ( 
                            SELECT DISTINCT 
                            DATE_TRUNC('month', a.createdat) AS mes 
                            ,a.user_id
                            FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                            WHERE 1=1
                            AND a.updated_by_id IS NULL
                            AND a.state = 'DONE'  
                            AND a.user_id not in (18144,17626,1343730, 1343764)
                            AND operation_country = 'ARG'
                            AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                            AND a.transaction_type = 'LEMON_CARD_PAYMENT'
                            ) f ON a.mes = f.mes AND a.user_id = f.user_id
                
                WHERE 1=1
                GROUP BY 1
                ORDER BY 1 DESC) infotxprodstrat
            
    full join
    --misma query pero para los NEW users
        (       SELECT 
             a.mes as mes2
    
            ,COUNT(DISTINCT CASE WHEN (b.user_id IS NOT NULL OR bb.user_id IS NOT NULL
                                   OR  c.user_id IS NOT NULL 
                                   OR  d.user_id IS NOT NULL)
                                   AND f.user_id IS     NULL THEN a.user_id ELSE NULL END) AS NEW_CRYPTO_USER
    
            ,COUNT(DISTINCT CASE WHEN  b.user_id IS     NULL 
                                  AND bb.user_id IS     NULL
                                   AND c.user_id IS     NULL 
                                   AND d.user_id IS     NULL 
                                   AND f.user_id IS NOT NULL THEN a.user_id ELSE NULL END) AS NEW_CARD_USER
    
            ,COUNT(DISTINCT CASE WHEN  b.user_id IS     NULL  
                                  AND bb.user_id IS     NULL
                                   AND c.user_id IS     NULL 
                                   AND d.user_id IS     NULL 
                                   AND e.user_id IS NOT NULL
                                   AND f.user_id IS     NULL THEN a.user_id ELSE NULL END) AS NEW_CRYPTO_JUST_EARN_USER
            
            ,COUNT(DISTINCT CASE WHEN (b.user_id IS NOT NULL OR bb.user_id IS NOT NULL
                                   OR  c.user_id IS NOT NULL 
                                   OR  d.user_id IS NOT NULL)
                                   AND f.user_id IS NOT NULL THEN a.user_id ELSE NULL END) AS NEW_CRYPTO_CARD_USER
                
    
            FROM (  SELECT DISTINCT 
                    DATE_TRUNC('month', a.createdat) AS mes
                    ,a.user_id  
                    FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id                  
                    WHERE 1=1
                    AND a.updated_by_id IS NULL
                    AND a.state = 'DONE'  
                    AND a.user_id not in (18144,17626,1343730, 1343764)
                    AND operation_country = 'ARG'
                    AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                    and a.createdat <= (b.createdat + interval '30 day')
                    AND a.transaction_type IN ('CRYPTO_PURCHASE','CRYPTO_SALE','CRYPTO_SWAP','INTEREST_EARNING','LEMON_CARD_PAYMENT')) a
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id 
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'CRYPTO_SALE'
                        and a.createdat <= (b.createdat + interval '30 day')
                        AND a.visible = 1
                        ) b ON a.mes = b.mes AND a.user_id = b.user_id
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'CRYPTO_SALE'
                        and a.createdat <= (b.createdat + interval '30 day')
                        AND COALESCE(a.visible,0) = 0
                        ) bb ON a.mes = bb.mes AND a.user_id = bb.user_id
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'CRYPTO_PURCHASE'
                        and a.createdat <= (b.createdat + interval '30 day')
                        ) c ON a.mes = c.mes AND a.user_id = c.user_id
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'CRYPTO_SWAP'
                        and a.createdat <= (b.createdat + interval '30 day')
                        ) d ON a.mes = d.mes AND a.user_id = d.user_id
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'INTEREST_EARNING'
                        and a.createdat <= (b.createdat + interval '30 day')
                        ) e ON a.mes = e.mes AND a.user_id = e.user_id
            LEFT JOIN ( 
                        SELECT DISTINCT 
                        DATE_TRUNC('month', a.createdat) AS mes 
                        ,a.user_id
                        FROM lemoncash_ar.activities a inner join lemoncash_ar.users b on a.user_id = b.id
                        WHERE 1=1
                        AND a.updated_by_id IS NULL
                        AND a.state = 'DONE'  
                        AND a.user_id not in (18144,17626,1343730, 1343764)
                        AND operation_country = 'ARG'
                        AND CAST(a.createdat AS DATE) >= DATE '2022-01-01'
                        AND a.transaction_type = 'LEMON_CARD_PAYMENT'
                        and a.createdat <= (b.createdat + interval '30 day')
                        ) f ON a.mes = f.mes AND a.user_id = f.user_id
                WHERE 1=1
                GROUP BY 1
                ORDER BY 1 DESC) newtxusersprodstrat on newtxusersprodstrat.mes2 = infotxprodstrat.mes
    ) prodstrat on prodstrat.mes = a.mes
    
    left join
    
    (select date_trunc('month',createdat) as mes, count(distinct user_id) as eop_total_sale_users
    from lemoncash_ar.activities
    where transaction_type = 'CRYPTO_SALE'
    group by 1
    order by 1
    ) totaluserssale on totaluserssale.mes = a.mes
    
    left join
    
    (
    SELECT 
       count(distinct act.user_id) AS new_total_sale_transacting_users,
       date_trunc('month',  u.createdat)  as mes,
        'new sale transacting users' as tipo 
       
    FROM  lemoncash_ar.users u 
    inner join  lemoncash_ar.activities act on act.user_id = u.id
    
    where 
        act.createdat <= (u.createdat + interval '30 day')
        and (transaction_type = 'CRYPTO_SALE')
    
    and user_id not in (18144,17626,1343730, 1343764)
    and operation_country = 'ARG'
     
    GROUP BY 2
    
    order by 2
    ) newtotsale on newtotsale.mes = a.mes
where a.mes >= '2022-01-01'
ORDER BY 1 DESC )
                     """)

    
    truncate_metricas_prodstrat >> insert_metricas_prodstrat


