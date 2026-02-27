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
),

movers AS (
    SELECT
        coin_id,
        symbol,
        name,
        current_price_usd,
        pct_change_24h,
        pct_change_7d,
        total_volume_usd,
        CASE
            WHEN pct_change_24h > 0 THEN 'gainer'
            WHEN pct_change_24h < 0 THEN 'loser'
            ELSE 'flat'
        END AS mover_type,
        ingested_at
    FROM latest
    WHERE rn = 1
      AND pct_change_24h IS NOT NULL
)

SELECT * FROM movers
ORDER BY ABS(pct_change_24h) DESC
LIMIT 20
