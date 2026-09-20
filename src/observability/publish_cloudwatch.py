"""Publish aggregate SentinelPay migration health metrics to CloudWatch.

The script reads existing local audit reports. It never uploads source records,
customer identifiers, or transaction-level data. Use --execute only after the
dry-run output has been reviewed.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


DEFAULT_NAMESPACE = "SentinelPay/Migration"


def load_json(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise FileNotFoundError(f"Report not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_metrics(
    quality_report: dict[str, object], reconciliation_report: dict[str, object]
) -> list[dict[str, object]]:
    valid_rows = quality_report.get("valid_rows")
    quarantined_rows = quality_report.get("quarantined_rows")
    target = reconciliation_report.get("target")
    if not isinstance(valid_rows, dict) or not isinstance(quarantined_rows, dict) or not isinstance(target, dict):
        raise ValueError("Reports do not have the expected quality and reconciliation fields.")

    processed_rows = sum(int(value) for value in valid_rows.values())
    quarantined_total = sum(int(value) for value in quarantined_rows.values())
    quality_completed = quality_report.get("status") == "COMPLETED"
    reconciliation_passed = reconciliation_report.get("status") == "PASSED"

    return [
        {"MetricName": "PipelineSuccess", "Value": 1 if quality_completed and reconciliation_passed else 0, "Unit": "Count"},
        {"MetricName": "RecordsProcessed", "Value": processed_rows, "Unit": "Count"},
        {"MetricName": "ValidPayments", "Value": int(target["valid_payment_count"]), "Unit": "Count"},
        {"MetricName": "QuarantinedRecords", "Value": quarantined_total, "Unit": "Count"},
        {"MetricName": "ReconciliationPassed", "Value": 1 if reconciliation_passed else 0, "Unit": "Count"},
        {"MetricName": "QualityChecksCompleted", "Value": 1 if quality_completed else 0, "Unit": "Count"},
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish aggregate migration metrics to CloudWatch.")
    parser.add_argument("--quality-report", type=Path)
    parser.add_argument("--reconciliation-report", type=Path)
    parser.add_argument(
        "--pipeline-status",
        choices=("success", "failure"),
        default="success",
        help="Use failure for an Airflow task-failure callback.",
    )
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE", "sentinelpay-deployer"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-1"))
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--execute", action="store_true", help="Publish metrics. Omit for a dry run.")
    args = parser.parse_args()

    if args.pipeline_status == "failure":
        metrics = [{"MetricName": "PipelineSuccess", "Value": 0, "Unit": "Count"}]
    else:
        if args.quality_report is None or args.reconciliation_report is None:
            parser.error("--quality-report and --reconciliation-report are required for a successful pipeline run.")
        quality_report = load_json(args.quality_report)
        reconciliation_report = load_json(args.reconciliation_report)
        metrics = build_metrics(quality_report, reconciliation_report)
    dimensions = [{"Name": "Pipeline", "Value": "sentinelpay-migration"}]
    metric_data = [{**metric, "Dimensions": dimensions} for metric in metrics]

    mode = "PUBLISH" if args.execute else "DRY RUN"
    print(f"{mode}: namespace={args.namespace}, region={args.region}, metrics={len(metric_data)}")
    for metric in metric_data:
        print(f"{metric['MetricName']}: {metric['Value']} {metric['Unit']}")

    if not args.execute:
        print("No CloudWatch metrics published. Review this output, then rerun with --execute.")
        return

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cloudwatch = session.client("cloudwatch")
    try:
        cloudwatch.put_metric_data(Namespace=args.namespace, MetricData=metric_data)
    except (BotoCoreError, ClientError) as error:
        raise RuntimeError("CloudWatch metric publication failed.") from error

    print("CloudWatch metrics published successfully.")


if __name__ == "__main__":
    main()
