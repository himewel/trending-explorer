from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include import TweepyExtractor


_RAW_PATH = "/data/raw"

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
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    tags=["Twitter", "Spark", "Tweepy"],
) as dag:
    extractor = TweepyExtractor(path=_RAW_PATH)
    extraction_task = PythonOperator(
        task_id="tweet_extraction",
        python_callable=extractor.fetch,
        op_kwargs={"topic": "#covid19", "max_results": "5"},
    )
