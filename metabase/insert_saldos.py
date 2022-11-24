from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from datetime import datetime, timedelta

with DAG(
        dag_id="insert_saldos",
        start_date=datetime(2022, 10, 11),
        schedule_interval='*/5 * * * *',
        tags=['insert', 'saldos', 'finance'],
        catchup=False,
) as dag:
    insert_saldos = RedshiftSQLOperator(
        task_id = 'insert_saldos',
        sql = """
    insert into finance.saldos (SELECT
    token,
    
    '|' AS "|",
    
    LEMON_WALLET_USD AS "Lemon Wallets usd",
    BINANCE_BROKER_USD AS "Binance Broker usd",
    BINANCE_TRADING_USD AS "Binance Trading usd",
    SETTLEMENTS_USD AS "Earn usd",
    FB_WITHDRAWALS_USD AS "Withdrawal usd",
    OTHERS_USD AS "Others usd",
    --CASE WHEN token != '0. MONEY' THEN VAULD_WITHDRAW ELSE NULL END AS VaulD_Retiros,
    
    '|' AS "||",
    
    BINANCE_TRADING_USD + BINANCE_BROKER_USD + FB_WITHDRAWALS_USD + SETTLEMENTS_USD + OTHERS_USD AS "Total Assets usd",
    BINANCE_TRADING_USD + BINANCE_BROKER_USD + FB_WITHDRAWALS_USD + SETTLEMENTS_USD + OTHERS_USD - LEMON_WALLET_USD AS "Saldos usd",
    
    '|' AS "|||",
    
    LEMON_WALLET AS Lemon_Wallets,
    BINANCE_BROKER AS "Binance Broker",
    BINANCE_TRADING AS "Binance Trading",
    SETTLEMENTS AS "Earn",
    FB_WITHDRAWALS AS "Withdrawal",
    OTHERS AS "Others",
    --CASE WHEN token != '0. MONEY' THEN VAULD_WITHDRAW ELSE NULL END AS VaulD_Retiros,
    
    '|' AS "||||",
    
    BINANCE_TRADING + BINANCE_BROKER + FB_WITHDRAWALS + SETTLEMENTS + OTHERS AS "Total Assets",
    BINANCE_TRADING + BINANCE_BROKER + FB_WITHDRAWALS + SETTLEMENTS + OTHERS - LEMON_WALLET AS "Saldos",
    current_timestamp as fecha_snap
    
    

FROM
    (
    SELECT
        token,
        CAST(LEMON_WALLET  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS LEMON_WALLET_USD,
        CAST(BINANCE_BROKER  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS BINANCE_BROKER_USD,
        CAST(BINANCE_TRADING  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS BINANCE_TRADING_USD,
        CAST(SETTLEMENTS  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS SETTLEMENTS_USD,
        CAST(FB_WITHDRAWALS  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS FB_WITHDRAWALS_USD,
        CAST(OTHERS  AS DECIMAL(36,8)) * CAST(coalesce(CAST(r.buy_price AS DECIMAL(36,8)), 1/CAST(r2.buy_price AS DECIMAL(36,8)), 1) AS DECIMAL(36,8)) AS OTHERS_USD,        
        CAST(LEMON_WALLET  AS DECIMAL(36,8)) AS LEMON_WALLET,
        CAST(BINANCE_BROKER  AS DECIMAL(36,8)) AS BINANCE_BROKER,
        CAST(BINANCE_TRADING  AS DECIMAL(36,8)) AS BINANCE_TRADING,
        CAST(SETTLEMENTS  AS DECIMAL(36,8)) AS SETTLEMENTS,
        CAST(FB_WITHDRAWALS  AS DECIMAL(36,8)) AS FB_WITHDRAWALS,
        CAST(OTHERS  AS DECIMAL(36,8)) AS OTHERS
      
        
    FROM
        (
        SELECT
            token,
            SUM(CASE WHEN wallet = 'LEMON_WALLET' THEN amount ELSE 0 END) AS LEMON_WALLET,
            SUM(CASE WHEN wallet = 'BINANCE_BROKER' THEN amount ELSE 0 END) AS BINANCE_BROKER,
            SUM(CASE WHEN wallet = 'BINANCE_TRADING' THEN amount ELSE 0 END) AS BINANCE_TRADING,

            sum ( case when wallet = 'Coinbase Exchange Lanin Pay' then amount else 0 end) +
            sum ( case when wallet = 'OkCoin Lanin Pay' then amount else 0 end) +
            sum ( case when wallet = 'hot wallets' then amount else 0 end) +
            sum ( case when wallet = 'open node' then amount else 0 end) as OTHERS, --OTHERS = coinbase + okcoin sin colocar + hot wallets + Open node
            
            sum ( case when wallet = 'binance flex' then amount else 0 end) +
            sum ( case when wallet = 'binance locked' then amount else 0 end) +
            sum ( case when wallet = 'okcoin' then amount else 0 end) +
            sum ( case when wallet = 'FTX Lanin Pay' then amount else 0 end) +
            sum ( case when wallet = 'alameda' then amount else 0 end) AS SETTLEMENTS, -- SETTLEMENTS = binance locked + binance flex + okcoin colocado + alameda
            SUM(CASE WHEN wallet = 'FB_WITHDRAWALS' THEN amount ELSE 0 END) AS FB_WITHDRAWALS
            
        
        FROM
            
            (
                -- externalcollectoraccountassets
                (
                SELECT asset.currency AS token, acc.name AS wallet, sum(cast(asset.amount AS decimal(36,8))) AS amount
                FROM lemoncash_ar.externalcollectoraccountassets asset
                LEFT JOIN lemoncash_ar.externalcollectoraccounts acc ON asset.external_collector_account_id = acc.id
                --WHERE asset.currency IN ('DAI','USDT','BTC','ETH','USDC')
                GROUP BY asset.currency, acc.name
                ORDER BY asset.currency ASC, acc.name ASC
                )
            
                UNION ALL
        
                -- Exchanges integrados a Fireblocks (coinbase, okcoin sin colocar, FTX)
                (
                select
                    a.currency as token, a.name as wallet, a.total as amount --, a.lockedamount, a.available, '1970-01-01 00:00:00 GMT'::timestamp +  __hevo__loaded_at / 1000 * interval '1 second' as fecha, a.id
                from fireblocks.accounts_fireblocks_insights a
                right join
                    (
                    select
                    currency, max (__hevo__ingested_at) as time, id, name
                    from fireblocks.accounts_fireblocks_insights a
                    WHERE (id IS NOT NULL AND (id <> '' OR id IS NULL))
                    group by 1,3,4
                    order by 1,3,4
                    ) b on a.currency = b.currency and a.__hevo__ingested_at = b.time and a.id = b.id and a.name = b.name
                order by 1,2 desc
                )
                
                UNION ALL
                
                -- Posiciones crypto (Alameda, okcoin, binance locked, binance flex, hot wallets, Open node): https://docs.google.com/spreadsheets/d/1vOpoqlr_9N8_CM-Rtaxth1uhQe_Gg1xoZA-QFL0CavY/edit#gid=1570597025
                (
                SELECT
                currency as token, platform as wallet, sum(treasury_hevo_settlements.amount) as amount
                FROM finance.treasury_hevo_settlements
                GROUP BY 1,2
                )            
                            
                UNION ALL
                
                -- LEMON WALLETS TOKENS
                
                (
                SELECT 
                --CASE WHEN w.AssetTypeId = 'MONEY' THEN '0. MONEY' ELSE w.AssetTypeId END AS token, 'LEMON_WALLET' AS wallet, 
                w.AssetTypeId as token, 'LEMON_WALLET' AS wallet, sum(cast(w.balance AS decimal(36,8))) AS amount
                FROM lemoncash_ar.wallets w
                --WHERE w.AssetTypeId IN ('DAI','USDT','BTC','ETH','USDC')
                GROUP BY w.AssetTypeId
                ORDER BY w.AssetTypeId ASC
                ) 
                
                UNION ALL
                
                -- FIREBLOCKS WITHDRAWALS TOKENS
                
                (
                select
                    currency as token, 'FB_WITHDRAWALS' as wallet, sum(balance) as amount
                from
                    (
                    select
                        a.currency, a.network, a.balance, '1970-01-01 00:00:00 GMT'::timestamp +  __hevo__loaded_at / 1000 * interval '1 second' as fecha, a.vaultaccountid
                        --, '1970-01-01 00:00:00 GMT'::timestamp +  __hevo__ingested_at / 1000 * interval '1 second' as fecha2, '1970-01-01 00:00:00 GMT'::timestamp +  __hevo__source_modified_at / 1000 * interval '1 second' as fecha3
                    from fireblocks.accounts_fireblocks_insights a
                    right join
                        (
                        select
                        currency, network, max (__hevo__ingested_at) as time, vaultaccountid
                        from fireblocks.accounts_fireblocks_insights a
                        where vaultaccountid is not null
                        group by 1,2,4
                        order by 1,2,4
                        ) b on a.currency = b.currency and a.network = b.network and a.__hevo__ingested_at = b.time and a.vaultaccountid = b.vaultaccountid
                    WHERE a.vaultaccountid IN ('3','2039466','2039468')                   
                    order by 1,2 desc
                    )
                group by 1
                )
                
            ) a 
        GROUP BY token
        ) b
    
    LEFT JOIN lemoncash_ar.ExchangeRatesV2 r ON r.base_currency = b.token AND r.quote_currency = 'USD'
    LEFT JOIN lemoncash_ar.ExchangeRatesV2 r2 ON r2.base_currency = 'USD' AND r2.quote_currency = b.token

    ORDER BY token ASC
    ) b    

WHERE token NOT IN ('MONEY','JUICE','BRL')
ORDER BY 3 DESC)"""

    )
insert_saldos 
