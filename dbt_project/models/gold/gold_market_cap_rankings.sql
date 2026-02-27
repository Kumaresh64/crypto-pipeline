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
    market_cap_rank,
    coin_id,
    symbol,
    name,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    pct_change_24h,
    pct_change_7d,
    ingested_at
FROM latest
WHERE rn = 1
ORDER BY market_cap_rank ASC
