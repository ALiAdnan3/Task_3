"""
Currency Exchange Rate ETL pipeline.

Extract   -> fetch the latest exchange rates from the Frankfurter API
Transform -> reshape each rate into the required document format
Load      -> upsert the documents into MongoDB

Data moves between tasks with *named* XComs (explicit keys).
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pymongo import MongoClient, ReplaceOne

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]

# "mongodb" is the Docker service name from docker-compose.override.yml
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Field order of each document stored in MongoDB
DOCUMENT_FIELDS = [
    "base_currency",
    "target_currency",
    "exchange_rate",
    "rate_date",
    "source",
    "retrieved_at",
]

# Named XCom keys
RAW_RATES_KEY = "raw_exchange_rates"
TRANSFORMED_RATES_KEY = "transformed_exchange_rates"


# ---------------------------------------------------------------------------
# Task 1: Extract
# ---------------------------------------------------------------------------
def extract_exchange_rates(**context):
    """Call the Frankfurter API and push the raw JSON to XCom."""
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_rates = response.json()

    # Example response:
    # [{"date": "2026-11-10", "base": "EUR", "quote": "USD", "rate": 1.1208}, ...]
    if not isinstance(raw_rates, list) or not raw_rates:
        raise ValueError(f"Unexpected API response: {raw_rates}")

    log.info("Extracted %d rates: %s", len(raw_rates), raw_rates)
    context["ti"].xcom_push(key=RAW_RATES_KEY, value=raw_rates)


# ---------------------------------------------------------------------------
# Task 2: Transform
# ---------------------------------------------------------------------------
def transform_exchange_rates(**context):
    """Pull raw rates from XCom, reshape them, push the result to XCom."""
    raw_rates = context["ti"].xcom_pull(
        task_ids="extract_exchange_rates", key=RAW_RATES_KEY
    )
    if not raw_rates:
        raise ValueError("No raw exchange rates found in XCom")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    transformed = [
        {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": float(item["rate"]),
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at,
        }
        for item in raw_rates
    ]

    log.info("Transformed %d records: %s", len(transformed), transformed)
    context["ti"].xcom_push(key=TRANSFORMED_RATES_KEY, value=transformed)


# ---------------------------------------------------------------------------
# Task 3: Load
# ---------------------------------------------------------------------------
def load_to_mongodb(**context):
    """Pull transformed records from XCom and upsert them into MongoDB."""
    records = context["ti"].xcom_pull(
        task_ids="transform_exchange_rates", key=TRANSFORMED_RATES_KEY
    )
    if not records:
        raise ValueError("No transformed exchange rates found in XCom")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # One document per currency pair per day -> re-running a day
        # updates the existing documents instead of creating duplicates.
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
        )
        operations = [
            ReplaceOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                # XCom storage (Postgres JSONB) reorders keys, so rebuild
                # each document in the required field order.
                {field: r[field] for field in DOCUMENT_FIELDS},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)
        log.info(
            "Loaded into %s.%s -> inserted: %d, updated: %d",
            MONGO_DB,
            MONGO_COLLECTION,
            result.upserted_count,
            result.modified_count,
        )
    finally:
        client.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_exchange_rates,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    extract_task >> transform_task >> load_task
