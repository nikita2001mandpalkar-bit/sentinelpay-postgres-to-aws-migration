"""Clean and validate a local Bronze full-load snapshot into Silver Parquet datasets."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def read_bronze(spark: SparkSession, input_root: Path, table: str) -> DataFrame:
    path_pattern = str(input_root / table / "extract_date=*" / "batch_id=*" / "records.csv")
    return spark.read.option("header", True).option("inferSchema", True).csv(path_pattern)


def add_quarantine_metadata(dataframe: DataFrame, table: str, reason: str) -> DataFrame:
    return (
        dataframe.withColumn("rejection_reason", F.lit(reason))
        .withColumn("source_table", F.lit(table))
        .withColumn("quarantined_at", F.lit(utc_now()))
    )


def write_parquet(dataframe: DataFrame, path: Path, partition_column: str | None = None) -> None:
    writer = dataframe.write.mode("errorifexists")
    if partition_column:
        writer.partitionBy(partition_column).parquet(str(path))
    else:
        writer.parquet(str(path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform a local Bronze run into Silver Parquet.")
    parser.add_argument("--input-run-id", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/output/bronze"))
    parser.add_argument("--silver-dir", type=Path, default=Path("data/output/silver"))
    parser.add_argument("--quarantine-dir", type=Path, default=Path("data/output/quarantine"))
    args = parser.parse_args()

    run_id = args.run_id or f"silver-{args.input_run_id}"
    input_root = args.bronze_dir / f"run_id={args.input_run_id}"
    silver_root = args.silver_dir / f"run_id={run_id}"
    quarantine_root = args.quarantine_dir / f"run_id={run_id}"
    if not input_root.exists():
        raise FileNotFoundError(f"Bronze run does not exist: {input_root}")
    if silver_root.exists() or quarantine_root.exists():
        raise FileExistsError("Silver or quarantine output already exists. Use a new run ID to avoid duplicate output.")

    spark = (
        SparkSession.builder.appName("SentinelPayBronzeToSilver")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        merchants = read_bronze(spark, input_root, "merchants")
        customers = read_bronze(spark, input_root, "customers")
        payments = read_bronze(spark, input_root, "payments")
        settlements = read_bronze(spark, input_root, "settlements")

        merchant_invalid = merchants.filter(F.col("merchant_id").isNull() | (F.trim("merchant_id") == ""))
        merchant_valid = merchants.join(merchant_invalid.select("merchant_id"), "merchant_id", "left_anti")

        customer_invalid = customers.filter(F.col("customer_id").isNull() | (F.trim("customer_id") == ""))
        customer_valid = customers.join(customer_invalid.select("customer_id"), "customer_id", "left_anti")

        payments_with_merchants = payments.join(
            merchant_valid.select(F.col("merchant_id").alias("known_merchant_id")),
            payments.merchant_id == F.col("known_merchant_id"),
            "left",
        )
        payment_reason = (
            F.when(F.col("payment_id").isNull() | (F.trim("payment_id") == ""), "MISSING_PAYMENT_ID")
            .when(F.col("payment_amount").isNull() | (F.col("payment_amount") <= 0), "INVALID_PAYMENT_AMOUNT")
            .when(F.col("known_merchant_id").isNull(), "MISSING_MERCHANT_REFERENCE")
        )
        payments_checked = payments_with_merchants.withColumn("rejection_reason", payment_reason)
        payment_invalid = payments_checked.filter(F.col("rejection_reason").isNotNull()).drop("known_merchant_id")
        payment_candidates = payments_checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "known_merchant_id")
        payment_window = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("source_record_id").desc())
        payment_ranked = payment_candidates.withColumn("duplicate_rank", F.row_number().over(payment_window))
        payment_duplicates = payment_ranked.filter(F.col("duplicate_rank") > 1).drop("duplicate_rank")
        payment_valid = payment_ranked.filter(F.col("duplicate_rank") == 1).drop("duplicate_rank")

        settlements_with_payments = settlements.join(
            payment_valid.select(F.col("payment_id").alias("known_payment_id")),
            settlements.payment_id == F.col("known_payment_id"),
            "left",
        )
        settlement_reason = (
            F.when(F.col("settlement_id").isNull() | (F.trim("settlement_id") == ""), "MISSING_SETTLEMENT_ID")
            .when(F.col("settlement_amount").isNull() | (F.col("settlement_amount") <= 0), "INVALID_SETTLEMENT_AMOUNT")
            .when(F.col("known_payment_id").isNull(), "MISSING_PAYMENT_REFERENCE")
        )
        settlements_checked = settlements_with_payments.withColumn("rejection_reason", settlement_reason)
        settlement_invalid = settlements_checked.filter(F.col("rejection_reason").isNotNull()).drop("known_payment_id")
        settlement_valid = settlements_checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "known_payment_id")

        quality_report = {
            "run_id": run_id,
            "input_run_id": args.input_run_id,
            "status": "COMPLETED",
            "processed_at": utc_now(),
            "valid_rows": {
                "merchants": merchant_valid.count(),
                "customers": customer_valid.count(),
                "payments": payment_valid.count(),
                "settlements": settlement_valid.count(),
            },
            "quarantined_rows": {
                "merchants": merchant_invalid.count(),
                "customers": customer_invalid.count(),
                "payments_invalid": payment_invalid.count(),
                "payments_duplicates": payment_duplicates.count(),
                "settlements": settlement_invalid.count(),
            },
        }

        write_parquet(merchant_valid, silver_root / "merchants")
        write_parquet(customer_valid, silver_root / "customers")
        write_parquet(payment_valid.withColumn("payment_date", F.to_date("payment_timestamp")), silver_root / "payments", "payment_date")
        write_parquet(settlement_valid.withColumn("settlement_date", F.to_date("settlement_timestamp")), silver_root / "settlements", "settlement_date")

        write_parquet(add_quarantine_metadata(merchant_invalid, "merchants", "MISSING_MERCHANT_ID"), quarantine_root / "merchants")
        write_parquet(add_quarantine_metadata(customer_invalid, "customers", "MISSING_CUSTOMER_ID"), quarantine_root / "customers")
        write_parquet(payment_invalid.withColumn("source_table", F.lit("payments")).withColumn("quarantined_at", F.lit(utc_now())), quarantine_root / "payments_invalid")
        write_parquet(add_quarantine_metadata(payment_duplicates, "payments", "DUPLICATE_PAYMENT_ID"), quarantine_root / "payments_duplicates")
        write_parquet(settlement_invalid.withColumn("source_table", F.lit("settlements")).withColumn("quarantined_at", F.lit(utc_now())), quarantine_root / "settlements")

        silver_root.mkdir(parents=True, exist_ok=True)
        (silver_root / "quality_report.json").write_text(json.dumps(quality_report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(quality_report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
