from airflow import DAG, settings, secrets
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta,datetime
import os

with DAG(
    dag_id="daily_tiers", 
    start_date=days_ago(1), 
    schedule_interval='20 0 */4 * *', 
    tags=['tiers']) as dag:

    insert_tiers= RedshiftSQLOperator(
        task_id='insert_daily_tiers', 
        sql=""" INSERT INTO lemoncash_data.tiers (select a.user_id, volumen_purchase, volumen_sale, volumen_swap, volumen_card,
        cantidad_purchase, cantidad_sale, cantidad_swap, cantidad_card,
        saldo, saldo_earn,
    case when (saldo_earn >= 40000) or (volumen_swap >= 100000 or volumen_sale >= 60000 or volumen_purchase >= 60000) then 1
        when (saldo_earn between 4000 and 40000) or (volumen_swap between 20000 and 100000 or volumen_sale between 6000 and 60000 or volumen_purchase between 6000 and 60000) then 2
        when ((saldo_earn between 1000 and 4000) or (volumen_swap between 4000 and 20000 or volumen_sale between 1500 and 6000 or volumen_purchase between 1500 and 6000)) and volumen_card >= 90 then 3
        else 4 end as tiers
    ,current_date as fecha_snapshot
from 
(         SELECT
            user_id,
            SUM(case when transaction_type = 'CRYPTO_PURCHASE' THEN CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8)) else 0 end)  AS volumen_purchase,
            SUM(case when transaction_type = 'CRYPTO_SALE' THEN CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8)) else 0 end)  AS volumen_sale,
            SUM(case when transaction_type = 'LEMON_CARD_PAYMENT' THEN CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8)) else 0 end)  AS volumen_card,
            SUM(case when transaction_type = 'CRYPTO_SWAP' THEN CAST(suma_mes AS DECIMAL(36,8)) * CAST(rate_to_usd AS DECIMAL(36,8)) else 0 end)  AS volumen_swap,
            SUM(case when transaction_type = 'CRYPTO_PURCHASE' THEN CAST(cantidad_mes AS DECIMAL(36,8)) else 0 end)  AS cantidad_purchase,
            SUM(case when transaction_type = 'CRYPTO_SALE' THEN CAST(cantidad_mes AS DECIMAL(36,8)) else 0 end)  AS cantidad_sale,
            SUM(case when transaction_type = 'LEMON_CARD_PAYMENT' THEN CAST(cantidad_mes AS DECIMAL(36,8)) else 0 end)  AS cantidad_card,
            SUM(case when transaction_type = 'CRYPTO_SWAP' THEN CAST(cantidad_mes AS DECIMAL(36,8)) else 0 end)  AS cantidad_swap
        
         FROM 
                (
                    SELECT
                        a.user_id,
                        a.transaction_type,
                        a.currency,
                        DATE_TRUNC('hour', a.createdAt) AS mes,
                        SUM(CAST(a.amount AS DECIMAL(36,8))) AS suma_mes,
                        count(distinct a.transaction_id) as cantidad_mes
                    
                    FROM
                        lemoncash_ar.activities a
                    
                    WHERE
                        a.updated_by_id IS NULL
                        AND (a.state = 'DONE' OR (a.state = 'PENDING' and a.transaction_type IN ('WALLET_TO_EXTERNAL_WALLET', 'VIRTUAL_WITHDRAWAL', 'WALLET_TO_BANK') ) )
                        AND a.user_id NOT IN (18144,17626,1343730, 1343764)
                        AND ((a.transaction_type = 'CRYPTO_PURCHASE' AND a.operation_type = 'DEBIT') 
                            OR a.transaction_type = 'CRYPTO_SALE' AND a.operation_type = 'DEBIT'
                            OR a.transaction_type = 'LEMON_CARD_PAYMENT' 
                            OR a.transaction_type = 'CRYPTO_SWAP' AND a.operation_type = 'DEBIT'
                            OR a.transaction_type = 'INTEREST_EARNING')
                    GROUP BY 1,2,3,4
                    having mes < CURRENT_DATE AND mes > CURRENT_DATE - 30
                ) a

         INNER JOIN
            
         -- COTIZACION MONEDAS 
            (
                (
                SELECT
                    DATE_TRUNC('hour', a.fecha) AS fecha,
                    a.currency,
                    AVG(CAST(p.purchase_price_amount AS DECIMAL(36,8))) AS rate_to_usd
                
                FROM
                    (
                        SELECT
                            DISTINCT date_trunc('hour', created_at) AS fecha,
                            purchase_currency AS currency
                        
                        FROM
                            lemoncash_ar.cryptopurchasetransactions
                        
                        ORDER BY fecha
                    ) a
                    INNER JOIN lemoncash_ar.cryptopurchasetransactions p ON a.fecha = DATE_TRUNC('hour', p.created_at) AND a.currency = p.purchase_currency
                
                GROUP BY 1,2
                ORDER BY 1
                )
                UNION ALL
                (
                                     SELECT DATE_TRUNC('hour', a.fecha)                                             AS fecha,
                                            a.currency,
                                            AVG(1 / CAST(p.usd_price_in_spending_fiat_currency AS DECIMAL(36, 8))) AS rate_to_usd
        
                                     FROM (
                                              SELECT DISTINCT DATE_TRUNC('hour', created_at) AS fecha,
                                                              'MONEY'                       AS currency
        
                                              FROM lemoncash_ar.cryptopurchasetransactions
        
                                              ORDER BY fecha
                                          ) a
                                              INNER JOIN lemoncash_ar.cryptopurchasetransactions p
                                                         ON a.fecha = DATE_TRUNC('hour', p.created_at)
        
                                     GROUP BY 1, 2
                                     ORDER BY 1
                                 )
            ) c ON a.mes = c.fecha AND a.currency = c.currency
        
         GROUP BY 1
         ORDER BY 1,2) a 
        inner join 
        
        
     (select userid as user_id, sum(balance_usdd) as saldo
      from ((select userid, w.assettypeid, (ex.sell_price * w.balance) as balance_usdd
            from lemoncash_ar.wallets w
                     inner join lemoncash_ar.exchangeratesv2 ex on w.assettypeid = ex.base_currency
            where ex.quote_currency = 'USD'
              and userid not in (select a.id
                                 from lemoncash_ar.userscash a
                                          inner join "lemoners"."hoja 1" b on a.tax_id = b.cuil)
              AND userid NOT IN (18144,17626,1343730, 1343764)
              and w.balance > 0
            order by 1)
            
            union all
            
            (select userid, w.assettypeid, (w.balance/ex.buy_price) as balance_usdd
            from lemoncash_ar.wallets w
                     inner join lemoncash_ar.exchangeratesv2 ex on w.assettypeid = ex.quote_currency
            where ex.quote_currency = 'MONEY'
              and userid not in (select a.id
                                 from lemoncash_ar.userscash a
                                          inner join "lemoners"."hoja 1" b on a.tax_id = b.cuil)
              AND userid NOT IN (18144, 17626)
              and w.balance >= 0
            order by 1))
            
      group by 1
      order by 1) b on a.user_id = b.user_id
      
      inner join 
      
      (select userid as user_id, sum(balance_usdd) as saldo_earn
      from (select userid, w.assettypeid, (ex.sell_price * w.balance) as balance_usdd
            from lemoncash_ar.wallets w
                     inner join lemoncash_ar.exchangeratesv2 ex on w.assettypeid = ex.base_currency
            where ex.quote_currency = 'USD'
            and ex.base_currency in ('DAI','USDT','USDC','BTC','ETH','SOL','DOT','ADA')
              and userid not in (select a.id
                                 from lemoncash_ar.userscash a
                                          inner join "lemoners"."hoja 1" b on a.tax_id = b.cuil)
              AND userid NOT IN (18144,17626,1343730, 1343764)
              and w.balance >= 0 
            order by 1)
      group by 1
      order by 1) c on a.user_id = c.user_id
order by 1) """)

    
    insert_tiers