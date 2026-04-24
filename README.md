#  Crypto Market Analytics Pipeline

An end-to-end data engineering pipeline that ingests real-time cryptocurrency market data, transforms it through a medallion architecture, and orchestrates everything automatically with Apache Airflow.

---

## Architecture
```
CoinGecko API
      ↓
Python Ingestion Script
      ↓
DuckDB (Bronze Layer — raw data)
      ↓
dbt (Silver → Gold transformations)
      ↓
Analytics-Ready Tables
```

## Data Models

| Layer | Model | Description |
|-------|-------|-------------|
|  Bronze | `bronze_crypto_prices` | Raw API response landed as-is |
|  Silver | `silver_crypto_prices` | Cleaned, typed, deduplicated |
|  Gold | `gold_market_cap_rankings` | Top 50 coins ranked by market cap |
|  Gold | `gold_top_movers` | Biggest 24h gainers & losers |
|  Gold | `gold_volatility_scores` | Volatility scores & bands per coin |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Data ingestion & pipeline logic |
| DuckDB | Local analytical database |
| dbt | Data transformation & testing |
| Apache Airflow | Pipeline orchestration |
| CoinGecko API | Free real-time crypto market data |
| Git & GitHub | Version control |

---

## Project Structure
```
crypto-pipeline/
│
├── ingestion/
│   └── ingest_crypto.py        # Hits CoinGecko API, lands to DuckDB bronze
├── dbt_project/
│   ├── models/
│   │   ├── bronze/             # Raw source views
│   │   ├── silver/             # Cleaned & typed tables
│   │   └── gold/               # Business-ready analytics tables
│   ├── profiles.yml            # DuckDB connection config
│   └── dbt_project.yml         # dbt project config
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py  # Hourly orchestration DAG
├── data/                       # DuckDB database (gitignored)
├── logs/                       # Pipeline logs (gitignored)
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
└── README.md
```

---

##  Setup & Run Locally

### 1. Clone the repo
```bash
git clone https://github.com/Kumaresh64/crypto-pipeline.git
cd crypto-pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
```bash
cp .env.example .env
```

### 5. Run the ingestion pipeline
```bash
python ingestion/ingest_crypto.py
```

### 6. Run dbt transformations
```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 7. Start Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
airflow db migrate
airflow standalone
```

---

##  Pipeline Flow

The Airflow DAG runs **every hour** and executes these tasks in order:
```
[ingest_crypto_data] → [dbt_run] → [dbt_test]
```

1. **ingest_crypto_data** — Pulls top 50 coins from CoinGecko and lands raw data into DuckDB bronze table
2. **dbt_run** — Transforms bronze → silver → gold models
3. **dbt_test** — Validates data quality across all models

---

##  Sample Data
```
coin_id  | symbol | current_price_usd | pct_change_24h | volatility_band
---------+--------+-------------------+----------------+----------------
bitcoin  | BTC    | 62,450.00         | +2.34%         | MEDIUM
ethereum | ETH    | 3,421.00          | -1.12%         | LOW
solana   | SOL    | 142.50            | +8.75%         | HIGH
```

---

## Roadmap

- [ ] Add Docker & docker-compose for full containerisation
- [ ] Add Great Expectations for data quality checks
- [ ] Add Metabase / Superset dashboard layer
- [ ] Deploy to cloud (GCP / AWS)
- [ ] Add historical trend analysis models

---

## Author

**Kumaresh**
- GitHub: [@Kumaresh64](https://github.com/Kumaresh64)

---

## 📝 License

MIT License
