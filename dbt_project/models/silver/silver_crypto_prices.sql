WITH source AS (
    SELECT * FROM bronze_crypto_prices
),

cleaned AS (
    SELECT
        coin_id,
        symbol,
        name,
        ROUND(current_price_usd, 6)      AS current_price_usd,
        ROUND(high_24h_usd, 6)           AS high_24h_usd,
        ROUND(low_24h_usd, 6)            AS low_24h_usd,
        ROUND(price_change_24h, 6)       AS price_change_24h,
        CAST(market_cap_usd AS BIGINT)   AS market_cap_usd,
        CAST(market_cap_rank AS INT)     AS market_cap_rank,
        CAST(total_volume_usd AS BIGINT) AS total_volume_usd,
        ROUND(pct_change_24h, 4)         AS pct_change_24h,
        ROUND(pct_change_7d, 4)          AS pct_change_7d,
        circulating_supply,
        total_supply,
        max_supply,
        ROUND(ath_usd, 6)                AS ath_usd,
        CAST(ath_date AS VARCHAR)        AS ath_date,
        CAST(last_updated AS VARCHAR)    AS last_updated,
        CAST(ingested_at AS TIMESTAMP)   AS ingested_at
    FROM source
    WHERE coin_id IS NOT NULL
      AND current_price_usd IS NOT NULL
      AND current_price_usd > 0
)

SELECT * FROM cleaned
