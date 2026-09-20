"""Reconcile the legacy PostgreSQL source with the Gold migration target."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def decimal_value(value: Decimal | None) -> str:
    return str(value or Decimal("0.00"))


def source_metrics(dsn: str) -> dict[str, int | str]:
    """Calculate the source state using the same quality and latest-record rules as Silver."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM payments")
        raw_payment_count = cursor.fetchone()[0]

        cursor.execute(
            """
            WITH eligible AS (
                SELECT
                    p.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.payment_id
                        ORDER BY p.updated_at DESC NULLS LAST, p.source_record_id DESC
                    ) AS duplicate_rank
                FROM payments p
                INNER JOIN merchants m ON p.merchant_id = m.merchant_id
                WHERE p.payment_id IS NOT NULL
                  AND BTRIM(p.payment_id) <> ''
                  AND p.payment_amount > 0
            )
            SELECT COUNT(*), COALESCE(SUM(payment_amount), 0)
            FROM eligible
            WHERE duplicate_rank = 1
            """
        )
        accepted_count, accepted_amount = cursor.fetchone()

    return {
        "raw_payment_count": raw_payment_count,
        "expected_valid_payment_count": accepted_count,
        "expected_valid_payment_amount": decimal_value(accepted_amount),
    }


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Reconcile a SentinelPay source migration run.")
    parser.add_argument("--silver-run-id", required=True)
    parser.add_argument("--gold-run-id", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/output/audit"))
    args = parser.parse_args()

    dsn = os.getenv("SENTINELPAY_DB_DSN")
    if not dsn:
        raise ValueError("SENTINELPAY_DB_DSN is required. Copy .env.example to .env first.")

    silver_root = Path("data/output/silver") / f"run_id={args.silver_run_id}"
    gold_root = Path("data/output/gold") / f"run_id={args.gold_run_id}"
    quality_report_path = silver_root / "quality_report.json"
    if not quality_report_path.exists() or not gold_root.exists():
        raise FileNotFoundError("The supplied Silver or Gold run does not exist.")

    quality_report = json.loads(quality_report_path.read_text(encoding="utf-8"))
    source = source_metrics(dsn)

    spark = SparkSession.builder.appName("SentinelPayMigrationReconciliation").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        target_row = (
            spark.read.parquet(str(gold_root / "payment_reconciliation"))
            .agg(
                F.count("payment_id").alias("payment_count"),
                F.round(F.sum("payment_amount"), 2).alias("payment_amount"),
                F.round(F.sum("settled_amount"), 2).alias("settled_amount"),
            )
            .first()
        )
    finally:
        spark.stop()

    target_count = target_row["payment_count"]
    target_amount = Decimal(str(target_row["payment_amount"] or 0)).quantize(Decimal("0.01"))
    source_amount = Decimal(source["expected_valid_payment_amount"]).quantize(Decimal("0.01"))
    quarantined_payment_rows = (
        quality_report["quarantined_rows"]["payments_invalid"]
        + quality_report["quarantined_rows"]["payments_duplicates"]
    )

    checks = {
        "raw_records_accounted_for": source["raw_payment_count"] == target_count + quarantined_payment_rows,
        "valid_payment_count_matches": source["expected_valid_payment_count"] == target_count,
        "valid_payment_amount_matches": source_amount == target_amount,
    }
    report = {
        "silver_run_id": args.silver_run_id,
        "gold_run_id": args.gold_run_id,
        "reconciled_at": utc_now(),
        "status": "PASSED" if all(checks.values()) else "FAILED",
        "checks": checks,
        "source": source,
        "target": {
            "valid_payment_count": target_count,
            "valid_payment_amount": str(target_amount),
            "settled_amount": str(target_row["settled_amount"] or 0),
            "quarantined_payment_rows": quarantined_payment_rows,
        },
    }

    output_path = args.output_dir / f"gold_run_id={args.gold_run_id}"
    output_path.mkdir(parents=True, exist_ok=False)
    report_path = output_path / "migration_reconciliation.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if report["status"] != "PASSED":
        raise SystemExit("Migration reconciliation failed. See the audit report for details.")


if __name__ == "__main__":
    main()
