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
    dag_id="insert_revenues_lemon", 
    start_date=days_ago(1), 
    schedule_interval='10 00 * * *', 
    on_failure_callback = slack_util.dag_failure_notification(
        channel = CHANNEL,
        users = OWNERS,
    ),
    tags=['tabla','revenues','mensual']) as dag:

    truncate_revenues = RedshiftSQLOperator(
        task_id='truncate_user_revenues', 
        sql="TRUNCATE lemoncash_data.revenues_lemon;"
    )
    

    insert_revenues = RedshiftSQLOperator(
        task_id='insert_revenues', 
        sql=""" INSERT INTO lemoncash_data.revenues_lemon 
(with a as 
    (select a.mes, purchase, sale, swap, fee_amount_usd as card, coalesce(revenue_earn,0) as earn
    from
    
        (select date_trunc('month',a.dia) as mes, sum(revenue_purchase) as purchase, sum(revenue_sale) as sale, sum(revenue_swap) as swap
        from lemoncash_data.user_revenues a
        where date_trunc('month',a.dia) >= '2022-01-01'
        group by 1
        order by 1 desc
        ) a
    
    full join 
    
        (SELECT SUM(cast(vauld_interest as decimal(16,8))*b.buy_price) as revenue_earn,
                'INTEREST_EARNING' as concept_code,
                DATE_TRUNC('month',pay_date) as mes
        FROM        
            (
            SELECT
                CAST( date_trunc('day',payout_date__inst::DATE) AS DATE) AS pay_date,
                currency,
                sum(paid_interest) AS vauld_interest
            FROM
                finance_vauldinterest.vauld_interest
            WHERE
                status = 'matured'
            GROUP BY 1,2
            order by 1
            ) a
        
        left JOIN 
            (SELECT DATE_TRUNC('day', created_at) AS fechacotizacion,
                    currency AS base_currency, 
                    AVG(cast(crypto_sale_price_amount as decimal(16,8))) AS buy_price
            FROM  lemoncash_ar.cryptosaletransactions
            GROUP BY 1,2) b 
        ON b.base_currency = a.currency and b.fechacotizacion = a.pay_date
        
        GROUP BY 2,3
        ORDER BY 3) b
    on a.mes = b.mes
    
    full join 
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
                SUM(case when a.currency = 'USD' then (fee_amount*(c.billete_venta/1.1))/ccl
                        when a.currency = 'MONEY' then fee_amount/ccl end) as fee_amount_usd,
                    concept_code,
                    date_trunc('month',dia) as mes
        FROM a
        full JOIN b 
            on date_trunc('month', a.dia) = b.mes
        full join c
            on date_trunc('day', c.fecha) = a.dia
        where mes >= '2022-01-01'
        group by 2,3
        order by 3 desc) c
        on c.mes = a.mes
    
    order by 1), 

b as 
    (select dia,
        crypto_purchase_ars,
        crypto_purchase_ars_sincomis,
        crypto_purchase_USD,
        avg_fx_crypto_purchase,
        crypto_sale_ars,
        crypto_Sale_usd,
        avg_fx_crypto_sale,
        crypto_ars_trade,
        crypto_usd_trade,
        non_cash_trading,
        daily_spread,
        spread_revenue_ars,
        spread_revenue_usd,
        crypto_ars_trade_pending,
        crypto_usd_trade_pending,
        operating_cash_balance_ars_correcto as operating_cash_balance_ars,
        operating_cash_balance_usd,
        fx_stock_pre_alyc_correcto as fx_stock_pre_alyc,
        ccl_purchase_ars,
        ccl_purchase_usd,
        avg_fx_ccl_purchase,
        ccl_sale_ars,
        ccl_sale_usd,
        avg_fx_ccl_sale,
        ccl_trade_ars,
        ccl_trade_usd,
        finance_cash_balance_ars,
        finance_cash_balance_usd,
        ccl_fx_2 as ccl_fx,
        lemon_cash_balance_ars,
        lemon_cash_balance_usd,
        revaluacion_ars,
        revaluacion_usd,
        resultado_arbitraje_usd
    from
        (select dia,
            crypto_purchase_ars,
            crypto_purchase_ars_sincomis,
            crypto_purchase_USD,
            avg_fx_crypto_purchase,
            crypto_sale_ars,
            crypto_Sale_usd,
            avg_fx_crypto_sale,
            crypto_ars_trade,
            crypto_usd_trade,
            non_cash_trading,
            daily_spread,
            spread_revenue_ars,
            spread_revenue_usd,
            crypto_ars_trade_pending,
            crypto_usd_trade_pending,
            case when dia <= '2021-01-05' then operating_cash_balance_ars else (crypto_ars_trade_pending + lag(lemon_cash_balance_ars) over (order by dia asc)) end as operating_cash_balance_ars_correcto,
            operating_cash_balance_usd,
            --fx_stock_pre_alyc as fx_stock_pre_alyc_no_es_Correcto,
            operating_cash_balance_ars_correcto/operating_cash_balance_usd as fx_stock_pre_alyc_correcto,
            ccl_purchase_ars,
            ccl_purchase_usd,
            avg_fx_ccl_purchase,
            ccl_sale_ars,
            ccl_sale_usd,
            avg_fx_ccl_sale,
            ccl_trade_ars,
            ccl_trade_usd,
            finance_cash_balance_ars,
            finance_cash_balance_usd,
            ccl_fx_2,
            lemon_cash_balance_ars,
            lemon_cash_balance_usd,
            case when ccl_trade_usd != 0 then operating_cash_balance_usd*(fx_stock_pre_alyc_correcto+ccl_fx_2) else 0 end as revaluacion_ars,
            case when ccl_fx_2 != 0 then (revaluacion_ars/ccl_fx_2) else 0 end as revaluacion_usd,
            revaluacion_usd as resultado_arbitraje_usd
        from
            (with a as (
                select 
                    a.dia, 
                    crypto_purchase_ars, 
                    crypto_purchase_ars_sincomis,
                    crypto_purchase_USD, --SIN COMIS
                    avg_fx_crypto_purchase, 
                    coalesce((crypto_sale_ars * 0.995) * -1,0) as crypto_sale_ars,
                    coalesce((crypto_Sale_usd * 0.995),0) as crypto_sale_usd, 
                    coalesce(avg_fx_crypto_sale,0) as avg_fx_crypto_sale, 
                    coalesce(crypto_purchase_ars_sincomis - (crypto_sale_ars * 0.995),crypto_purchase_ars_sincomis) as crypto_ars_trade, 
                    coalesce((crypto_sale_usd * 0.995) + crypto_purchase_usd,crypto_purchase_usd)  as crypto_usd_trade, 
                    case when (crypto_purchase_usd*-1) < (crypto_sale_usd * 0.995) then crypto_purchase_usd * -1 else (crypto_sale_usd * 0.995) end as non_cash_trading,
                    avg_fx_crypto_purchase - avg_fx_crypto_sale as daily_spread,
                    non_cash_trading * daily_spread as spread_revenue_ars,
                    ( spread_revenue_ars) / avg_fx_crypto_sale  as spread_revenue_usd,
                    crypto_ars_trade as crypto_ars_trade_pending,
                    crypto_usd_trade as crypto_usd_trade_pending,
                    coalesce((e.volumen*-1),0) as ccl_purchase_ars,
                    coalesce(e.volumen/e.avg_price,0) as ccl_purchase_usd,
                    case when ccl_purchase_usd != 0 then coalesce(-1*ccl_purchase_ars/ccl_purchase_usd,0) else 0 end as avg_fx_ccl_purchase,
                    coalesce(d.volumen*d.avg_price,0) as ccl_sale_ars,
                    coalesce((d.volumen*-1),0) as ccl_sale_usd,
                    case when ccl_sale_usd != 0 then coalesce(-1*ccl_sale_ars/ccl_sale_usd,0) else 0 end as avg_fx_ccl_sale,
                    ccl_purchase_ars+ccl_sale_ars as ccl_trade_ars,
                    ccl_purchase_usd+ccl_sale_usd as ccl_trade_usd,
                    sum(ccl_trade_ars) over (order by a.dia rows unbounded preceding) as finance_cash_balance_ars,
                    sum(ccl_trade_usd) over (order by a.dia rows unbounded preceding) as finance_cash_balance_usd,
                    case when ccl_trade_usd != 0 then coalesce(-1*ccl_trade_ars/ccl_trade_usd,0) else 0 end as ccl_fx
                    
                from
                    (
                    select 
                        distinct date_trunc('day',createdat) as  dia  
                    from lemoncash_ar.activities
                    ) a 
                    
                LEFT JOIN 
                
                    (
                    SELECT 
                        sum(spending_money_amount) as crypto_purchase_ars, 
                        sum(spending_money_amount) * 0.99 as crypto_purchase_ars_sincomis,
                        sum((cast(spending_money_amount as decimal(36,8)) * 0.99 ) /  cast(usd_price_in_spending_fiat_currency as decimal(36,8))) * -1 as crypto_purchase_usd,
                        (crypto_purchase_ars_sincomis / crypto_purchase_usd)*-1 as avg_fx_crypto_purchase,
                        avg(usd_price_in_spending_fiat_currency) as promedio_usd,
                        sum(spending_money_amount)/ sum(cast(purchased_amount as decimal(36,8)) * cast(purchase_price_amount as decimal(36,8))) as avg_fx_crypto_purchase_2,
                        date_trunc('day',created_at) as dia 
                    from lemoncash_ar.cryptopurchasetransactions
                    group by 7
                    order by 7 asc
                    ) b on a.dia = b.dia
                    
                LEFT JOIN 
                
                    (
                    select 
                        sum(cast(income_amount as decimal(36,8))) as crypto_sale_ars,
                        sum(cast(amount as decimal(36,8)) * cast(crypto_sale_price_amount as decimal(36,8))) as crypto_sale_usd,
                        crypto_sale_ars / crypto_sale_usd as avg_fx_crypto_sale,
                        date_trunc('day',created_at) as dia 
                    from lemoncash_ar.cryptosaletransactions
                    group by 4
                    order by 4 asc 
                    ) c on a.dia = c.dia 
                    
                LEFT JOIN
                
                    (SELECT 
                        finance.treasury_hevo_alycs_usd_to_ars.date AS dia,
                        sum(cast(finance.treasury_hevo_alycs_usd_to_ars.theoric_rate as decimal(36,8)) * cast(finance.treasury_hevo_alycs_usd_to_ars.volume_usd as decimal(36,8))) / sum(finance.treasury_hevo_alycs_usd_to_ars.volume_usd) AS avg_price,
                        sum(finance.treasury_hevo_alycs_usd_to_ars.volume_usd) AS volumen
                    FROM finance.treasury_hevo_alycs_usd_to_ars
                    GROUP BY 1
                    ORDER BY finance.treasury_hevo_alycs_usd_to_ars.date ASC) d on d.dia = a.dia
                
                LEFT JOIN
                    (SELECT cast(fecha as date) as dia, sum(a.pxq)/sum(a.volume) AS avg_price, sum(a.volume) as volumen
                    FROM
                        (
                        SELECT 
                            finance.treasury_hevo_alycs_ars_to_usd.date AS fecha,
                            finance.treasury_hevo_alycs_ars_to_usd.volume AS volume, 
                            finance.treasury_hevo_alycs_ars_to_usd.price AS price,
                            finance.treasury_hevo_alycs_ars_to_usd.volume * finance.treasury_hevo_alycs_ars_to_usd.price AS pxq
                        FROM finance.treasury_hevo_alycs_ars_to_usd
                        ORDER BY finance.treasury_hevo_alycs_ars_to_usd.date DESC
                        ) a
                    GROUP BY 1
                    order by 1) e on e.dia = a.dia
                
                where (crypto_purchase_ars is not null or crypto_sale_ars is not null)
                    and a.dia <= '2021-01-05'
                order by 1 desc),
            
            b as (
                select 
                    a.dia, 
                    crypto_purchase_ars, 
                    crypto_purchase_ars_sincomis, 
                    crypto_purchase_usd, --SIN COMIS
                    avg_fx_crypto_purchase, 
                    coalesce((crypto_sale_ars * 0.995) * -1,0) as crypto_sale_ars,
                    coalesce((crypto_Sale_usd * 0.995),0) as crypto_sale_usd, 
                    coalesce(avg_fx_crypto_sale,0) as avg_fx_crypto_sale, 
                    coalesce(crypto_purchase_ars_sincomis - (crypto_sale_ars * 0.995),crypto_purchase_ars_sincomis) as crypto_ars_trade, 
                    coalesce((crypto_sale_usd * 0.995) + crypto_purchase_usd,crypto_purchase_usd)  as crypto_usd_trade, 
                    case when (crypto_purchase_usd*-1) < (crypto_sale_usd * 0.995) then crypto_purchase_usd * -1 else (crypto_sale_usd * 0.995) end as non_cash_trading,
                    avg_fx_crypto_purchase - avg_fx_crypto_sale as daily_spread,
                    non_cash_trading * daily_spread as spread_revenue_ars,
                    ( spread_revenue_ars) / avg_fx_crypto_sale  as spread_revenue_usd,
                    
                    crypto_ars_trade as crypto_ars_trade_pending,
                    crypto_usd_trade as crypto_usd_trade_pending,
                    
                    -- case when crypto_ars_trade > 0 then ((-1) * crypto_usd_trade) * avg_fx_crypto_purchase else crypto_ars_trade end as crypto_ars_trade_pending,
                    sum(crypto_ars_trade_pending) over (order by a.dia rows unbounded preceding) as v4_ars,
                    -- case when crypto_usd_trade*-1 > 0 then ((-1) * crypto_ars_trade) / avg_fx_crypto_sale else crypto_usd_trade end as crypto_usd_trade_pending,
                    sum(crypto_usd_trade_pending) over (order by a.dia rows unbounded preceding) as v4_usd,
                    
                    coalesce((e.volumen*-1),0) as ccl_purchase_ars,
                    coalesce(e.volumen/e.avg_price,0) as ccl_purchase_usd,
                    case when ccl_purchase_usd != 0 then coalesce(-1*ccl_purchase_ars/ccl_purchase_usd,0) else 0 end as avg_fx_ccl_purchase,
                    coalesce(d.volumen*d.avg_price,0) as ccl_sale_ars,
                    coalesce((d.volumen*-1),0) as ccl_sale_usd,
                    case when ccl_sale_usd != 0 then coalesce(-1*ccl_sale_ars/ccl_sale_usd,0) else 0 end as avg_fx_ccl_sale,
                    ccl_purchase_ars+ccl_sale_ars as ccl_trade_ars,
                    sum(ccl_trade_ars) over (order by a.dia rows unbounded preceding) - ccl_trade_ars as v3_ars,
                    ccl_purchase_usd+ccl_sale_usd as ccl_trade_usd,
                    sum(ccl_trade_usd) over (order by a.dia rows unbounded preceding) - ccl_trade_usd as v3_usd,
                    sum(ccl_trade_ars) over (order by a.dia rows unbounded preceding) + (-1416800) as finance_cash_balance_ars,
                    sum(ccl_trade_usd) over (order by a.dia rows unbounded preceding) + (9205.24) as finance_cash_balance_usd,
                    case when ccl_trade_usd != 0 then coalesce(-1*ccl_trade_ars/ccl_trade_usd,0) else null end as ccl_fx
            
                from
                    (
                    select 
                        distinct date_trunc('day',createdat) as  dia  
                    from lemoncash_ar.activities
                    ) a 
                    
                LEFT JOIN 
                
                    (
                    SELECT 
                        sum(spending_money_amount) as crypto_purchase_ars, 
                        sum(spending_money_amount) * 0.99 as crypto_purchase_ars_sincomis,
                        sum((cast(spending_money_amount as decimal(36,8)) * 0.99 ) /  cast(usd_price_in_spending_fiat_currency as decimal(36,8))) * -1 as crypto_purchase_usd,
                        (crypto_purchase_ars_sincomis / crypto_purchase_usd)*-1 as avg_fx_crypto_purchase,
                        avg(usd_price_in_spending_fiat_currency) as promedio_usd,
                        sum(spending_money_amount)/ sum(cast(purchased_amount as decimal(36,8)) * cast(purchase_price_amount as decimal(36,8))) as avg_fx_crypto_purchase_2,
                        date_trunc('day',created_at) as dia 
                    from lemoncash_ar.cryptopurchasetransactions
                    group by 7
                    order by 7 asc
                    ) b on a.dia = b.dia
                    
                LEFT JOIN 
                
                    (
                    select 
                        sum(cast(income_amount as decimal(36,8))) as crypto_sale_ars,
                        sum(cast(amount as decimal(36,8)) * cast(crypto_sale_price_amount as decimal(36,8))) as crypto_sale_usd,
                        crypto_sale_ars / crypto_sale_usd as avg_fx_crypto_sale,
                        date_trunc('day',created_at) as dia 
                    from lemoncash_ar.cryptosaletransactions
                    group by 4
                    order by 4 asc 
                    ) c on a.dia = c.dia 
                    
                LEFT JOIN
                
                    (SELECT 
                        finance.treasury_hevo_alycs_usd_to_ars.date AS dia,
                        sum(cast(finance.treasury_hevo_alycs_usd_to_ars.theoric_rate as decimal(36,8)) * cast(finance.treasury_hevo_alycs_usd_to_ars.volume_usd as decimal(36,8))) / sum(finance.treasury_hevo_alycs_usd_to_ars.volume_usd) AS avg_price,
                        sum(finance.treasury_hevo_alycs_usd_to_ars.volume_usd) AS volumen
                    FROM finance.treasury_hevo_alycs_usd_to_ars
                    GROUP BY 1
                    ORDER BY finance.treasury_hevo_alycs_usd_to_ars.date ASC) d on d.dia = a.dia
                
                LEFT JOIN
                    (SELECT cast(fecha as date) as dia, sum(a.pxq)/sum(a.volume) AS avg_price, sum(a.volume) as volumen
                    FROM
                        (
                        SELECT 
                            finance.treasury_hevo_alycs_ars_to_usd.date AS fecha,
                            finance.treasury_hevo_alycs_ars_to_usd.volume AS volume, 
                            finance.treasury_hevo_alycs_ars_to_usd.price AS price,
                            finance.treasury_hevo_alycs_ars_to_usd.volume * finance.treasury_hevo_alycs_ars_to_usd.price AS pxq
                        FROM finance.treasury_hevo_alycs_ars_to_usd
                        ORDER BY finance.treasury_hevo_alycs_ars_to_usd.date DESC
                        ) a
                    GROUP BY 1
                    order by 1) e on e.dia = a.dia
                
                where (crypto_purchase_ars is not null or crypto_sale_ars is not null)
                    and a.dia > '2021-01-05'
                order by 1),
            
            c as (select -42266.393406 as v5_usd, 6505316.257904 as v5_ars)
                
                
            select distinct *
            
            from (
            
                (select 
                    dia, 
                    crypto_purchase_ars, 
                    crypto_purchase_ars_sincomis, 
                    crypto_purchase_usd as crypto_purchase_usd,
                    avg_fx_crypto_purchase as avg_fx_crypto_purchase, 
                    crypto_sale_ars as crypto_sale_ars,
                    crypto_sale_usd, 
                    avg_fx_crypto_sale avg_fx_crypto_sale, 
                    crypto_ars_trade as crypto_ars_trade, 
                    crypto_usd_trade as crypto_usd_trade, 
                    non_cash_trading as non_cash_trading,
                    daily_spread as daily_spread,
                    spread_revenue_ars as spread_revenue_ars,
                    spread_revenue_usd,
                    crypto_ars_trade_pending crypto_ars_trade_pending,
                    crypto_usd_trade_pending as crypto_usd_trade_pending,
                    sum(case when crypto_sale_ars is null then crypto_purchase_ars else crypto_ars_trade_pending end) over (order by a.dia rows unbounded preceding) as operating_cash_balance_ars,
                    sum(case when crypto_sale_usd is null then crypto_purchase_usd else crypto_usd_trade_pending end ) over (order by a.dia rows unbounded preceding) as operating_cash_balance_usd,
                    case when operating_cash_balance_usd != 0 then (operating_cash_balance_ars/operating_cash_balance_usd) else 0 end as fx_stock_pre_alyc,
                    ccl_purchase_ars,
                    ccl_purchase_usd,
                    avg_fx_ccl_purchase,
                    ccl_sale_ars,
                    ccl_sale_usd,
                    avg_fx_ccl_sale,
                    ccl_trade_ars,
                    ccl_trade_usd,
                    finance_cash_balance_ars,
                    finance_cash_balance_usd,
                    ccl_fx as ccl_fx_2,
                    (operating_cash_balance_usd + ccl_trade_usd) as lemon_cash_balance_usd,
                    (-1 * ccl_fx * lemon_cash_balance_usd) as lemon_cash_balance_ars
                from a) 
            
            union all 
                (select 
                    dia, --1
                    crypto_purchase_ars, --2
                    crypto_purchase_ars_sincomis, --3
                    crypto_purchase_usd, --4
                    avg_fx_crypto_purchase, --5
                    crypto_sale_ars,--6
                    crypto_sale_usd, --7
                    avg_fx_crypto_sale, --8
                    crypto_ars_trade, --9
                    crypto_usd_trade, --10
                    non_cash_trading, --11
                    daily_spread, --12
                    spread_revenue_ars, --13
                    spread_revenue_usd, --14
                    crypto_ars_trade_pending, --15
                    crypto_usd_trade_pending, --16
                    sum(v5_ars + v4_ars + v3_ars) operating_cash_balance_ars, --17
                    sum(v5_usd + v4_usd + v3_usd) operating_cash_balance_usd, --18
                    case when operating_cash_balance_usd != 0 then (operating_cash_balance_ars/operating_cash_balance_usd) else 0 end as fx_stock_pre_alyc, --19
                    ccl_purchase_ars, --20
                    ccl_purchase_usd, --21
                    avg_fx_ccl_purchase, --22
                    ccl_sale_ars, --23
                    ccl_sale_usd, --24
                    avg_fx_ccl_sale, --25
                    ccl_trade_ars, --26
                    ccl_trade_usd, --27
                    finance_cash_balance_ars, --28
                    finance_cash_balance_usd, --29
                    --ccl_fx,
                    case when dia <= '2021-01-06' then 153.91
                    when dia = '2021-01-07' then 153.91
                    when dia > '2021-01-07'  then last_value(ccl_fx ignore nulls) over (order by dia asc rows unbounded preceding)
                    else ccl_fx end as ccl_fx_2, --30
                    operating_cash_balance_usd + ccl_trade_usd as lemon_cash_balance_usd, --31
                    -1 * ccl_fx_2 * lemon_cash_balance_usd as lemon_cash_balance_ars --32
                
                from b,c
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,20,21,22,23,24,25,26,27,28,29,ccl_fx
                ))
            
            
            where 1=1 
                /*and dia = '2022-09-07'
                or (dia >= '2022-07-10'
                    and dia <= '2022-07-18')
                or (dia >= '2021-01-03' 
                    and dia <= '2021-01-08')*/
            order by 1)
            
        order by 1)
    order by 1) --,
    
--NO FUNCIONA PORQUE HABRIA QUE CONECTAR LA BASE DE LEMON BI CON LA DE VENTI
/*c as 
    (SELECT str_to_date(concat(date_format(source.createdAt, '%Y-%m'), '-01'), '%Y-%m-%d') AS mes,
        sum(source.activo_sin_iva) AS venti
    FROM 
        (SELECT Orders.id AS id,
            Orders.createdAt AS createdAt,
            ((Payments.priceInCents / CASE WHEN 100.0 = 0 THEN NULL ELSE 100.0 END) / CASE WHEN 1.21 = 0 THEN NULL ELSE 1.21 END) AS activo_sin_iva,
            Payments.status AS Payments__status,
            Payments.method AS Payments__method,
            Payments.type AS Payments__type
        FROM Orders
        LEFT JOIN Events
            ON Orders.eventId = Events.id
        LEFT JOIN Payments
            ON Orders.id = Payments.orderId
        LEFT JOIN Producers
            ON Events.producerId = Producers.id
        ) source
    WHERE (source.Payments__type = 'fee'
        AND (source.Payments__status = 'paid'OR source.Payments__status = 'refunded') 
        AND (source.Payments__method = 'mercadopago' OR source.Payments__method = 'lemoncash') 
        AND source.createdAt >= convert_tz('2022-01-01 00:00:00.000', 'GMT', @@session.time_zone))
    GROUP BY 1
    ORDER BY 1 ASC) */
    
select a.*, sum(b.spread_revenue_usd) as spread, sum(b.resultado_arbitraje_usd) as arbitraje --, c.venti
from a
full join b on a.mes = date_trunc('month',b.dia)
--inner join c on a.mes = c.mes
where a.mes is not null
group by 1,2,3,4,5,6
order by 1 desc)
         """)

    
    truncate_revenues >> insert_revenues


