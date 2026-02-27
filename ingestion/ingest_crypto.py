import requests
import duckdb
import pandas as pd
from datetime import datetime, timezone
import logging
import os
import json

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ingestion.log")
    ]
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
API_URL      = "https://api.coingecko.com/api/v3"
DB_PATH      = "data/crypto.duckdb"
COINS_LIMIT  = 50       # top 50 coins by market cap
VS_CURRENCY  = "usd"

# ── API Call ─────────────────────────────────────────────────────────────────
def fetch_top_coins(limit: int = COINS_LIMIT) -> list[dict]:
    """Hit CoinGecko and return raw coin market data."""
    url = f"{API_URL}/coins/markets"
    params = {
        "vs_currency":            VS_CURRENCY,
        "order":                  "market_cap_desc",
        "per_page":               limit,
        "page":                   1,
        "sparkline":              False,
        "price_change_percentage": "24h,7d"
    }

    log.info(f"Fetching top {limit} coins from CoinGecko...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()   # will throw if API returns an error

    data = response.json()
    log.info(f"Successfully fetched {len(data)} coins")
    return data


# ── Transform raw → DataFrame ─────────────────────────────────────────────────
def parse_coins(raw: list[dict]) -> pd.DataFrame:
    """Pull out only the fields we care about and add metadata."""
    records = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for coin in raw:
        records.append({
            # Identity
            "coin_id":            coin.get("id"),
            "symbol":             coin.get("symbol", "").upper(),
            "name":               coin.get("name"),

            # Market data
            "current_price_usd":  coin.get("current_price"),
            "market_cap_usd":     coin.get("market_cap"),
            "market_cap_rank":    coin.get("market_cap_rank"),
            "total_volume_usd":   coin.get("total_volume"),
            "high_24h_usd":       coin.get("high_24h"),
            "low_24h_usd":        coin.get("low_24h"),
            "price_change_24h":   coin.get("price_change_24h"),
            "pct_change_24h":     coin.get("price_change_percentage_24h"),
            "pct_change_7d":      coin.get("price_change_percentage_7d_in_currency"),

            # Supply
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply":       coin.get("total_supply"),
            "max_supply":         coin.get("max_supply"),

            # All-time highs
            "ath_usd":            coin.get("ath"),
            "ath_date":           coin.get("ath_date"),

            # Metadata
            "last_updated":       coin.get("last_updated"),
            "ingested_at":        ingested_at,
        })

    df = pd.DataFrame(records)
    log.info(f"Parsed {len(df)} records into DataFrame")
    return df


# ── Load into DuckDB ─────────────────────────────────────────────────────────
def load_to_duckdb(df: pd.DataFrame) -> None:
    """Create the bronze table if it doesn't exist and append new records."""
    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(DB_PATH)

    # Create schema if first run
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_crypto_prices (
            coin_id             VARCHAR,
            symbol              VARCHAR,
            name                VARCHAR,
            current_price_usd   DOUBLE,
            market_cap_usd      DOUBLE,
            market_cap_rank     INTEGER,
            total_volume_usd    DOUBLE,
            high_24h_usd        DOUBLE,
            low_24h_usd         DOUBLE,
            price_change_24h    DOUBLE,
            pct_change_24h      DOUBLE,
            pct_change_7d       DOUBLE,
            circulating_supply  DOUBLE,
            total_supply        DOUBLE,
            max_supply          DOUBLE,
            ath_usd             DOUBLE,
            ath_date            VARCHAR,
            last_updated        VARCHAR,
            ingested_at         VARCHAR
        )
    """)

    # Append new batch
    con.execute("INSERT INTO bronze_crypto_prices SELECT * FROM df")
    row_count = con.execute("SELECT COUNT(*) FROM bronze_crypto_prices").fetchone()[0]

    log.info(f"Loaded {len(df)} rows. Total rows in bronze table: {row_count}")
    con.close()


# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=== Crypto Ingestion Pipeline Starting ===")
    try:
        raw   = fetch_top_coins()
        df    = parse_coins(raw)
        load_to_duckdb(df)
        log.info("=== Pipeline Completed Successfully ===")
    except requests.exceptions.RequestException as e:
        log.error(f"API call failed: {e}")
        raise
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run()