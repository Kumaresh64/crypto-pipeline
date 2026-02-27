WITH silver AS (
    SELECT * FROM {{ ref('silver_crypto_prices') }}
),

latest AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY ingested_at DESC
        ) AS rn
    FROM silver
)

SELECT
    coin_id,
    symbol,
    name,
    current_price_usd,
    high_24h_usd,
    low_24h_usd,
    ROUND(
        (high_24h_usd - low_24h_usd) / NULLIF(low_24h_usd, 0) * 100
    , 4) AS volatility_pct,
    CASE
        WHEN ((high_24h_usd - low_24h_usd) / NULLIF(low_24h_usd, 0) * 100) > 10 THEN 'HIGH'
        WHEN ((high_24h_usd - low_24h_usd) / NULLIF(low_24h_usd, 0) * 100) > 5  THEN 'MEDIUM'
        ELSE 'LOW'
    END AS volatility_band,
    ingested_at
FROM latest
WHERE rn = 1
ORDER BY volatility_pct DESC
