"""Run Great Expectations checks against trusted Silver and Gold migration outputs."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def run_expectations(name: str, dataframe, expectations: list[object]) -> list[dict[str, object]]:
    """Validate an in-memory Spark DataFrame with an ephemeral GX context."""
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name=f"{name}_source")
    asset = datasource.add_dataframe_asset(name=f"{name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(name=f"{name}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})
    results = []
    for expectation in expectations:
        validation = batch.validate(expectation)
        results.append({"expectation": expectation.expectation_type, "success": validation.success})
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Great Expectations validation over SentinelPay outputs.")
    parser.add_argument("--silver-run-id", required=True)
    parser.add_argument("--gold-run-id", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--silver-dir", type=Path, default=Path("data/output/silver"))
    parser.add_argument("--gold-dir", type=Path, default=Path("data/output/gold"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/output/quality"))
    args = parser.parse_args()

    run_id = args.run_id or f"quality-{args.gold_run_id}"
    silver_root = args.silver_dir / f"run_id={args.silver_run_id}"
    gold_root = args.gold_dir / f"run_id={args.gold_run_id}"
    output_root = args.output_dir / f"run_id={run_id}"
    if not silver_root.exists() or not gold_root.exists():
        raise FileNotFoundError("The requested Silver or Gold run does not exist.")
    if output_root.exists():
        raise FileExistsError("Quality output already exists. Use a new run ID.")

    spark = SparkSession.builder.appName("SentinelPayGreatExpectations").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        merchants = spark.read.parquet(str(silver_root / "merchants"))
        payments = spark.read.parquet(str(silver_root / "payments"))
        reconciliation = spark.read.parquet(str(gold_root / "payment_reconciliation"))

        checks = {
            "merchants": run_expectations(
                "merchants",
                merchants,
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="merchant_id"),
                    gx.expectations.ExpectColumnValuesToBeUnique(column="merchant_id"),
                ],
            ),
            "payments": run_expectations(
                "payments",
                payments,
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="payment_id"),
                    gx.expectations.ExpectColumnValuesToBeUnique(column="payment_id"),
                    gx.expectations.ExpectColumnValuesToBeBetween(column="payment_amount", min_value=0.01),
                    gx.expectations.ExpectColumnValuesToBeInSet(
                        column="payment_status", value_set=["CAPTURED", "AUTHORIZED", "FAILED", "REFUNDED"]
                    ),
                ],
            ),
            "reconciliation": run_expectations(
                "reconciliation",
                reconciliation,
                [
                    gx.expectations.ExpectColumnValuesToNotBeNull(column="payment_id"),
                    gx.expectations.ExpectColumnValuesToBeInSet(
                        column="reconciliation_status",
                        value_set=["MATCHED", "MISSING_SETTLEMENT", "UNDER_SETTLED", "OVER_SETTLED"],
                    ),
                ],
            ),
        }
        orphan_payment_count = (
            payments.join(merchants.select("merchant_id"), "merchant_id", "left_anti").count()
        )
        checks["referential_integrity"] = [
            {"expectation": "payments.merchant_id_exists_in_merchants", "success": orphan_payment_count == 0}
        ]
        all_checks = [result["success"] for group in checks.values() for result in group]
        report = {
            "run_id": run_id,
            "silver_run_id": args.silver_run_id,
            "gold_run_id": args.gold_run_id,
            "validated_at": utc_now(),
            "status": "PASSED" if all(all_checks) else "FAILED",
            "checks": checks,
            "orphan_payment_count": orphan_payment_count,
        }
        output_root.mkdir(parents=True)
        (output_root / "great_expectations_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        if report["status"] != "PASSED":
            raise SystemExit("Great Expectations validation failed. See the report for details.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
