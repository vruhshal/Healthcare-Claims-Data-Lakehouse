"""
healthcare_pipeline_dag.py
Airflow DAG — orchestrates the full healthcare claims medallion pipeline.
Flow: check files → bronze ingest → DQ validate → silver transform → gold aggregate
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
    "on_failure_callback": lambda ctx: logging.error(
        f"FAILED: {ctx['task_instance'].dag_id}.{ctx['task_instance'].task_id}"
    ),
}


def check_source_files(**context):
    run_date = context["ds"]
    logging.info(f"Checking for new FHIR/HL7 files for {run_date}")
    # In production: use S3Hook to list s3://bucket/raw/{run_date}/
    file_count = 42  # mock
    if file_count == 0:
        raise ValueError(f"No source files found for {run_date}")
    logging.info(f"Found {file_count} files")
    return file_count


def run_great_expectations_suite(**context):
    """Run DQ suite against the silver layer. Fails DAG if score < 90%."""
    run_date = context["ds"]
    logging.info(f"Running Great Expectations suite for {run_date}")

    # Mock DQ results — in production use great_expectations Python API
    dq_results = {
        "null_claim_ids": 0,
        "negative_amounts": 0,
        "invalid_status_codes": 0,
        "invalid_icd10_codes": 2,      # a few might be bad
        "total_expectations": 30,
        "passed": 29,
    }

    score = dq_results["passed"] / dq_results["total_expectations"] * 100
    logging.info(f"DQ score: {score:.1f}%")

    if score < 90:
        raise ValueError(f"DQ score {score:.1f}% is below 90% threshold — blocking gold load")

    context["task_instance"].xcom_push(key="dq_score", value=score)
    logging.info("DQ passed.")


def send_slack_summary(**context):
    run_date = context["ds"]
    dq_score = context["task_instance"].xcom_pull(
        task_ids="run_dq_validation", key="dq_score"
    )
    logging.info(
        f"Pipeline complete for {run_date}. "
        f"DQ score: {dq_score:.1f}%. "
        f"Slack notification sent."
    )
    # In production: use SlackWebhookOperator or requests.post(SLACK_WEBHOOK_URL, ...)


with DAG(
    dag_id="healthcare_claims_pipeline",
    default_args=default_args,
    description="Daily healthcare claims medallion pipeline: raw → bronze → silver → gold",
    schedule_interval="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "medallion", "claims"],
) as dag:

    check_files = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
        sla=timedelta(minutes=10),
    )

    ingest_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command="""
            python /jobs/ingest/fhir_parser.py \
              --input s3://healthcare-bucket/raw/fhir/{{ ds }}/ \
              --output s3://healthcare-bucket/bronze/claims/
        """,
        sla=timedelta(minutes=25),
    )

    bronze_to_silver = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command="""
            spark-submit \
              --packages io.delta:delta-core_2.12:2.4.0 \
              /jobs/transform/bronze_to_silver.py \
              --input s3://healthcare-bucket/bronze/claims/ \
              --output s3://healthcare-bucket/silver/claims/
        """,
        sla=timedelta(minutes=45),
    )

    run_dq = PythonOperator(
        task_id="run_dq_validation",
        python_callable=run_great_expectations_suite,
    )

    silver_to_gold = BashOperator(
        task_id="transform_silver_to_gold",
        bash_command="""
            spark-submit \
              --packages io.delta:delta-core_2.12:2.4.0 \
              /jobs/transform/silver_to_gold.py \
              --input s3://healthcare-bucket/silver/claims/ \
              --output s3://healthcare-bucket/gold/
        """,
        sla=timedelta(minutes=55),
    )

    notify = PythonOperator(
        task_id="send_slack_summary",
        python_callable=send_slack_summary,
    )

    check_files >> ingest_bronze >> bronze_to_silver >> run_dq >> silver_to_gold >> notify
