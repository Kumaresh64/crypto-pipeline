# Crypto Market Analytics Pipeline

A modern, end-to-end data engineering solution that captures real-time cryptocurrency market data, processes it through a Medallion Architecture (Bronze, Silver, Gold), and delivers analytics-ready datasets through automated orchestration with Apache Airflow.

This project demonstrates core data engineering principles including ingestion, transformation, data quality validation, orchestration, and analytical modelling using industry-standard open-source technologies.

---

## Project Overview

The pipeline retrieves cryptocurrency market data from the CoinGecko API, stores raw records in a DuckDB Bronze layer, applies data cleansing and business transformations using dbt, and publishes curated Gold-layer datasets designed for analytics and reporting.

The entire workflow is orchestrated automatically using Apache Airflow, ensuring repeatable and reliable data processing.

---

## Architecture

```text
              CoinGecko API
                    │
                    ▼
        Python Data Ingestion Layer
                    │
                    ▼
        DuckDB Bronze (Raw Data)
                    │
                    ▼
         dbt Transformations
         Bronze → Silver → Gold
                    │
                    ▼
      Analytics & Reporting Tables
                    │
                    ▼
          Apache Airflow Scheduler
```

---

## Data Model Design

### Bronze Layer

Raw data ingested directly from the CoinGecko API with minimal processing for full auditability and traceability.

| Model | Description |
|---------|-------------|
| bronze_crypto_prices | Raw cryptocurrency market data landed directly from the API |

---

### Silver Layer

Standardised and cleansed datasets designed to improve data quality and consistency.

| Model | Description |
|---------|-------------|
| silver_crypto_prices | Typed, cleansed and deduplicated cryptocurrency data |

Key transformations include:

- Data type enforcement
- Null value handling
- Duplicate removal
- Column standardisation
- Data validation rules

---

### Gold Layer

Business-friendly analytical models designed for reporting and insight generation.

| Model | Description |
|---------|-------------|
| gold_market_cap_rankings | Top cryptocurrencies ranked by market capitalisation |
| gold_top_movers | Highest gainers and losers over the previous 24 hours |
| gold_volatility_scores | Calculated volatility metrics and risk bands |

---

## Technology Stack

| Technology | Purpose |
|------------|----------|
| Python | Data ingestion and pipeline logic |
| DuckDB | Embedded analytical database |
| dbt | Data transformation, modelling and testing |
| Apache Airflow | Workflow orchestration and scheduling |
| CoinGecko API | Real-time cryptocurrency market data |
| Git & GitHub | Version control and collaboration |

---

## Repository Structure

```text
crypto-pipeline/
│
├── ingestion/
│   └── ingest_crypto.py
│
├── dbt_project/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   │
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── airflow/
│   └── dags/
│       └── crypto_pipeline_dag.py
│
├── data/
├── logs/
│
├── .env.example
├── requirements.txt
└── README.md
```

---

## Local Setup

### Clone the Repository

```bash
git clone https://github.com/Kumaresh64/crypto-pipeline.git
cd crypto-pipeline
```

### Create a Virtual Environment

```bash
python -m venv venv

# Linux / Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Configure Environment Variables

```bash
cp .env.example .env
```

---

## Execute the Pipeline

### Run Data Ingestion

```bash
python ingestion/ingest_crypto.py
```

This step retrieves the latest cryptocurrency market data from CoinGecko and loads it into the Bronze layer.

---

### Execute dbt Transformations

```bash
cd dbt_project

dbt run --profiles-dir .
dbt test --profiles-dir .
```

This process:

1. Builds Silver models
2. Creates Gold analytical datasets
3. Executes data quality tests

---

### Launch Apache Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags

airflow db migrate
airflow standalone
```

---

## Orchestration Workflow

The Airflow DAG runs on an hourly schedule and executes the following workflow:

```text
ingest_crypto_data
          │
          ▼
        dbt_run
          │
          ▼
        dbt_test
```

### Task Breakdown

#### ingest_crypto_data

- Retrieves the latest market data from CoinGecko
- Loads records into DuckDB Bronze tables
- Captures ingestion metadata

#### dbt_run

- Executes Bronze-to-Silver transformations
- Builds Gold reporting models
- Applies business logic

#### dbt_test

- Runs schema and data quality tests
- Validates completeness and integrity
- Prevents invalid data from progressing

---

## Example Output

| Coin | Symbol | Current Price (USD) | 24H Change | Volatility Band |
|--------|---------|-------------------|-------------|----------------|
| Bitcoin | BTC | 62,450.00 | +2.34% | Medium |
| Ethereum | ETH | 3,421.00 | -1.12% | Low |
| Solana | SOL | 142.50 | +8.75% | High |

---

## Key Data Engineering Concepts Demonstrated

- Medallion Architecture
- ELT Data Processing
- Dimensional Modelling
- Incremental Data Loading
- Data Quality Validation
- Workflow Orchestration
- Automated Scheduling
- Analytical Data Modelling
- Version Control and CI-ready Development

---

## Future Enhancements

Planned improvements for the next iteration include:

- Docker and Docker Compose deployment
- Great Expectations data quality framework
- Historical trend and time-series modelling
- Cloud deployment (Azure, AWS or GCP)
- Real-time dashboard integration using Metabase or Apache Superset
- Automated CI/CD pipeline with GitHub Actions
- Data observability and monitoring

---

## Author

**Kumaresh Nallasamy**

- GitHub: https://github.com/Kumaresh64

---

## License

This project is released under the MIT License.

---

### Why This Project Matters

This project was built to showcase practical data engineering capabilities across the entire data lifecycle, from API ingestion through to curated analytical datasets. It reflects real-world patterns commonly used in modern analytics platforms and demonstrates proficiency in Python, SQL, dbt, Airflow, data modelling, orchestration, and automated data quality management.
