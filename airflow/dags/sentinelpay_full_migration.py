"""Manual Airflow orchestration for a complete SentinelPay historical migration."""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
RUN_ID = "airflow-full-{{ ts_nodash }}"
SILVER_RUN_ID = "airflow-silver-{{ ts_nodash }}"
GOLD_RUN_ID = "airflow-gold-{{ ts_nodash }}"

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def publish_failure_metric(context: dict) -> None:
    """Publish a zero only when an Airflow task has exhausted its retries."""
    subprocess.run(
        [
            str(PIPELINE_PYTHON),
            str(PROJECT_ROOT / "src/observability/publish_cloudwatch.py"),
            "--pipeline-status",
            "failure",
            "--execute",
        ],
        cwd=PROJECT_ROOT,
        check=False,
    )


with DAG(
    dag_id="sentinelpay_full_migration",
    description="Extract, validate, transform, and reconcile a legacy PostgreSQL payment warehouse.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=publish_failure_metric,
    tags=["sentinelpay", "migration", "local"],
) as dag:
    full_extract = BashOperator(
        task_id="full_extract_to_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} src/ingestion/extract_full_load.py "
            f"--run-id {RUN_ID} --batch-size 50000"
        ),
    )

    validate_bronze_manifest = BashOperator(
        task_id="validate_bronze_manifest",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} -m json.tool "
            f"data/output/bronze/run_id={RUN_ID}/manifest.json > /dev/null"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} src/transforms/bronze_to_silver.py "
            f"--input-run-id {RUN_ID} --run-id {SILVER_RUN_ID}"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} src/transforms/silver_to_gold.py "
            f"--input-run-id {SILVER_RUN_ID} --run-id {GOLD_RUN_ID}"
        ),
    )

    reconcile_source_to_target = BashOperator(
        task_id="reconcile_source_to_target",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} src/reconciliation/reconcile_migration.py "
            f"--silver-run-id {SILVER_RUN_ID} --gold-run-id {GOLD_RUN_ID}"
        ),
    )

    publish_cloudwatch_metrics = BashOperator(
        task_id="publish_cloudwatch_metrics",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PIPELINE_PYTHON} src/observability/publish_cloudwatch.py "
            f"--quality-report data/output/silver/run_id={SILVER_RUN_ID}/quality_report.json "
            f"--reconciliation-report data/output/audit/gold_run_id={GOLD_RUN_ID}/migration_reconciliation.json "
            "--execute"
        ),
    )

    full_extract >> validate_bronze_manifest >> bronze_to_silver >> silver_to_gold >> reconcile_source_to_target >> publish_cloudwatch_metrics
