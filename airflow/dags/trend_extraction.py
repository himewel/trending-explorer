from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include import TweepyExtractor, TweetIngestion, TweetRelease

_LANDING_PATH = "/data/landing"
_RAW_PATH = "/data/raw"
_TRUSTED_PATH = "/data/trusted"

default_args = {
    "depends_on_past": True,
    "retries": 5,
    "wait_for_downstream": True,
}

with DAG(
    dag_id="trending-extraction",
    default_args=default_args,
    description="Run automated data ingestion of tweets from trending topics",
    max_active_runs=1,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 10),
    tags=["Twitter", "Spark", "Tweepy", "DeltaLake"],
) as dag:
    extractor = TweepyExtractor(path=_LANDING_PATH)
    extraction_task = PythonOperator(
        task_id="extract_from_api",
        python_callable=extractor.fetch,
        op_kwargs={"topic": "#covid19", "max_results": 10},
    )

    ingestion = TweetIngestion(landing_path=_LANDING_PATH, raw_path=_RAW_PATH)
    ingestion_task = PythonOperator(
        task_id="ingest_to_raw",
        python_callable=ingestion.evaluate,
    )

    release = TweetRelease(raw_path=_RAW_PATH, trusted_path=_TRUSTED_PATH)
    release_task = PythonOperator(
        task_id="release_to_trusted",
        python_callable=release.evaluate,
    )

    extraction_task >> ingestion_task >> release_task
