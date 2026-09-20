"""Merge a Bronze incremental extract into a new current-state Silver snapshot."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def write_parquet(dataframe: DataFrame, path: Path, partition_column: str | None = None) -> None:
    writer = dataframe.write.mode("errorifexists")
    if partition_column:
        writer.partitionBy(partition_column).parquet(str(path))
    else:
        writer.parquet(str(path))


def read_incremental(spark: SparkSession, root: Path, table: str) -> DataFrame | None:
    table_root = root / table
    if not table_root.exists():
        return None
    return spark.read.option("header", True).option("inferSchema", True).csv(str(table_root / "batch_id=*" / "records.csv"))


def latest_by_key(dataframe: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(F.col("updated_at").desc_nulls_last(), F.col("source_record_id").desc_nulls_last())
    return dataframe.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") == 1).drop("_rank")


def quarantine(dataframe: DataFrame, table: str, reason: str) -> DataFrame:
    return dataframe.withColumn("rejection_reason", F.lit(reason)).withColumn("source_table", F.lit(table)).withColumn("quarantined_at", F.lit(utc_now()))


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge an incremental Bronze run into Silver.")
    parser.add_argument("--base-silver-run-id", required=True)
    parser.add_argument("--incremental-bronze-run-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--silver-dir", type=Path, default=Path("data/output/silver"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/output/bronze"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("data/output/quarantine"))
    args = parser.parse_args()

    base_root = args.silver_dir / f"run_id={args.base_silver_run_id}"
    bronze_root = args.bronze_dir / f"run_id={args.incremental_bronze_run_id}"
    output_root = args.silver_dir / f"run_id={args.run_id}"
    quarantine_root = args.quarantine_dir / f"run_id={args.run_id}"
    if not base_root.exists() or not bronze_root.exists():
        raise FileNotFoundError("The base Silver or incremental Bronze run does not exist.")
    if output_root.exists() or quarantine_root.exists():
        raise FileExistsError("Output already exists. Choose a new run ID to keep the merge idempotent.")

    spark = SparkSession.builder.appName("SentinelPayIncrementalSilverMerge").master("local[*]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        base_merchants = spark.read.parquet(str(base_root / "merchants"))
        base_customers = spark.read.parquet(str(base_root / "customers"))
        base_payments = spark.read.parquet(str(base_root / "payments"))
        base_settlements = spark.read.parquet(str(base_root / "settlements"))

        incremental_merchants = read_incremental(spark, bronze_root, "merchants")
        incremental_payments = read_incremental(spark, bronze_root, "payments")
        incremental_settlements = read_incremental(spark, bronze_root, "settlements")

        merchant_invalid = incremental_merchants.filter(F.col("merchant_id").isNull() | (F.trim("merchant_id") == ""))
        merchant_changes = incremental_merchants.filter(F.col("merchant_id").isNotNull() & (F.trim("merchant_id") != ""))
        merchant_current = latest_by_key(merchant_changes.withColumn("source_record_id", F.lit(None).cast("long")), "merchant_id").drop("source_record_id")
        merged_merchants = base_merchants.join(merchant_current.select("merchant_id"), "merchant_id", "left_anti").unionByName(merchant_current)

        if incremental_payments is None:
            raise ValueError("The incremental run contains no payment rows; no merge is required.")
        payments_with_merchants = incremental_payments.join(
            merged_merchants.select(F.col("merchant_id").alias("known_merchant_id")),
            incremental_payments.merchant_id == F.col("known_merchant_id"),
            "left",
        )
        payment_reason = (
            F.when(F.col("payment_id").isNull() | (F.trim("payment_id") == ""), "MISSING_PAYMENT_ID")
            .when(F.col("payment_amount").isNull() | (F.col("payment_amount") <= 0), "INVALID_PAYMENT_AMOUNT")
            .when(F.col("known_merchant_id").isNull(), "MISSING_MERCHANT_REFERENCE")
        )
        checked_payments = payments_with_merchants.withColumn("rejection_reason", payment_reason)
        payment_invalid = checked_payments.filter(F.col("rejection_reason").isNotNull()).drop("known_merchant_id")
        payment_candidates = checked_payments.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "known_merchant_id")
        incremental_payment_current = latest_by_key(payment_candidates, "payment_id")
        merged_payments = latest_by_key(
            base_payments.drop("payment_date").unionByName(incremental_payment_current), "payment_id"
        ).withColumn("payment_date", F.to_date("payment_timestamp"))

        if incremental_settlements is None:
            merged_settlements = base_settlements
            settlement_invalid = base_settlements.limit(0)
        else:
            settlements_with_payments = incremental_settlements.join(
                merged_payments.select(F.col("payment_id").alias("known_payment_id")),
                incremental_settlements.payment_id == F.col("known_payment_id"),
                "left",
            )
            settlement_reason = (
                F.when(F.col("settlement_id").isNull() | (F.trim("settlement_id") == ""), "MISSING_SETTLEMENT_ID")
                .when(F.col("settlement_amount").isNull() | (F.col("settlement_amount") <= 0), "INVALID_SETTLEMENT_AMOUNT")
                .when(F.col("known_payment_id").isNull(), "MISSING_PAYMENT_REFERENCE")
            )
            checked_settlements = settlements_with_payments.withColumn("rejection_reason", settlement_reason)
            settlement_invalid = checked_settlements.filter(F.col("rejection_reason").isNotNull()).drop("known_payment_id")
            settlement_candidates = checked_settlements.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "known_payment_id")
            merged_settlements = latest_by_key(
                base_settlements.drop("settlement_date").unionByName(settlement_candidates), "settlement_id"
            ).withColumn("settlement_date", F.to_date("settlement_timestamp"))

        report = {
            "run_id": args.run_id,
            "base_silver_run_id": args.base_silver_run_id,
            "incremental_bronze_run_id": args.incremental_bronze_run_id,
            "status": "COMPLETED",
            "processed_at": utc_now(),
            "incremental_rows": {
                "merchants": incremental_merchants.count(),
                "payments": incremental_payments.count(),
                "settlements": 0 if incremental_settlements is None else incremental_settlements.count(),
            },
            "merged_rows": {
                "merchants": merged_merchants.count(),
                "customers": base_customers.count(),
                "payments": merged_payments.count(),
                "settlements": merged_settlements.count(),
            },
            "quarantined_rows": {
                "merchants": merchant_invalid.count(),
                "payments": payment_invalid.count(),
                "settlements": settlement_invalid.count(),
            },
        }

        write_parquet(merged_merchants, output_root / "merchants")
        write_parquet(base_customers, output_root / "customers")
        write_parquet(merged_payments, output_root / "payments", "payment_date")
        write_parquet(merged_settlements, output_root / "settlements", "settlement_date")
        write_parquet(quarantine(merchant_invalid, "merchants", "MISSING_MERCHANT_ID"), quarantine_root / "merchants")
        write_parquet(payment_invalid.withColumn("source_table", F.lit("payments")).withColumn("quarantined_at", F.lit(utc_now())), quarantine_root / "payments")
        write_parquet(settlement_invalid.withColumn("source_table", F.lit("settlements")).withColumn("quarantined_at", F.lit(utc_now())), quarantine_root / "settlements")

        (output_root / "merge_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
