"""Generate a realistic legacy payments warehouse for migration testing."""

from __future__ import annotations

import argparse
import csv
import io
import os
import random
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
from dotenv import load_dotenv


BATCH_SIZE = 50_000


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def copy_rows(connection: psycopg.Connection, table: str, columns: list[str], rows: list[tuple]) -> None:
    """Bulk load a list of rows with PostgreSQL COPY instead of individual inserts."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerows(rows)
    buffer.seek(0)

    column_list = ", ".join(columns)
    with connection.cursor() as cursor:
        with cursor.copy(f"COPY {table} ({column_list}) FROM STDIN WITH (FORMAT CSV)") as copy:
            copy.write(buffer.read())


def generate_merchants(connection: psycopg.Connection, merchant_count: int, now: datetime) -> list[str]:
    merchant_ids = [f"mrc_{index:06d}" for index in range(1, merchant_count + 1)]
    categories = ["GROCERY", "TRAVEL", "RETAIL", "FOOD", "HEALTH", "ELECTRONICS"]
    cities = ["Mumbai", "Pune", "Bengaluru", "Delhi", "Hyderabad", "Chennai"]
    risk_tiers = ["LOW", "MEDIUM", "HIGH"]

    rows = [
        (
            merchant_id,
            f"Merchant {index:06d}",
            random.choice(categories),
            random.choice(cities),
            "INDIA",
            random.choices(risk_tiers, weights=[75, 20, 5])[0],
            now - timedelta(days=random.randint(365, 1_500)),
            now - timedelta(days=random.randint(0, 90)),
            "legacy_seed_001",
        )
        for index, merchant_id in enumerate(merchant_ids, start=1)
    ]
    copy_rows(
        connection,
        "merchants",
        ["merchant_id", "merchant_name", "merchant_category", "city", "state", "risk_tier", "created_at", "updated_at", "source_batch_id"],
        rows,
    )
    return merchant_ids


def generate_customers(connection: psycopg.Connection, customer_count: int, now: datetime) -> list[str]:
    customer_ids = [f"cus_{index:08d}" for index in range(1, customer_count + 1)]
    cities = ["Mumbai", "Pune", "Bengaluru", "Delhi", "Hyderabad", "Chennai"]

    rows = [
        (
            customer_id,
            f"Customer {index:08d}",
            f"customer{index:08d}@example.test",
            random.choice(cities),
            "INDIA",
            now - timedelta(days=random.randint(30, 1_500)),
            now - timedelta(days=random.randint(0, 90)),
            "legacy_seed_001",
        )
        for index, customer_id in enumerate(customer_ids, start=1)
    ]
    copy_rows(
        connection,
        "customers",
        ["customer_id", "customer_name", "email", "city", "state", "created_at", "updated_at", "source_batch_id"],
        rows,
    )
    return customer_ids


def generate_payments(
    connection: psycopg.Connection,
    payment_count: int,
    merchant_ids: list[str],
    customer_ids: list[str],
    now: datetime,
) -> None:
    """Generate source rows including duplicates, invalid amounts, and missing merchants."""
    statuses = ["CAPTURED", "AUTHORIZED", "FAILED", "REFUNDED"]
    settlements_total = 0

    for start in range(0, payment_count, BATCH_SIZE):
        payment_rows: list[tuple] = []
        settlement_rows: list[tuple] = []
        end = min(start + BATCH_SIZE, payment_count)
        for index in range(start, end):
            payment_id = f"pay_{index:010d}"
            if index > 0 and index % 10_000 == 0:
                payment_id = f"pay_{index - 1:010d}"  # Intentional duplicate business key.

            merchant_id = random.choice(merchant_ids)
            if index % 20_000 == 0:
                merchant_id = "mrc_missing"  # Intentional referential-quality failure.

            amount = round(random.uniform(50, 25_000), 2)
            if index % 25_000 == 0:
                amount = -amount  # Intentional invalid amount.

            payment_time = now - timedelta(days=random.randint(0, 730), minutes=random.randint(0, 1_440))
            updated_at = payment_time + timedelta(days=random.randint(0, 10))
            status = random.choices(statuses, weights=[80, 10, 7, 3])[0]
            payment_rows.append(
                (
                    payment_id,
                    random.choice(customer_ids),
                    merchant_id,
                    amount,
                    "INR",
                    status,
                    payment_time,
                    payment_time,
                    min(updated_at, now),
                    "legacy_seed_001",
                )
            )
            if amount > 0 and merchant_id != "mrc_missing" and random.random() >= 0.10:
                settlement_time = payment_time + timedelta(days=random.randint(1, 14))
                if random.random() < 0.02:
                    settlement_time += timedelta(days=45)  # Intentional late-arriving settlement.
                settlement_amount = amount if random.random() >= 0.02 else round(amount * 0.95, 2)
                settlement_rows.append(
                    (
                        f"stl_{uuid.uuid4().hex[:16]}",
                        payment_id,
                        merchant_id,
                        settlement_amount,
                        "SETTLED",
                        settlement_time,
                        payment_time,
                        min(settlement_time + timedelta(hours=random.randint(0, 24)), now),
                        "legacy_seed_001",
                    )
                )

        copy_rows(
            connection,
            "payments",
            [
                "payment_id",
                "customer_id",
                "merchant_id",
                "payment_amount",
                "currency",
                "payment_status",
                "payment_timestamp",
                "created_at",
                "updated_at",
                "source_batch_id",
            ],
            payment_rows,
        )
        copy_rows(
            connection,
            "settlements",
            ["settlement_id", "payment_id", "merchant_id", "settlement_amount", "settlement_status", "settlement_timestamp", "created_at", "updated_at", "source_batch_id"],
            settlement_rows,
        )
        connection.commit()
        settlements_total += len(settlement_rows)
        print(f"Loaded payments: {end:,}/{payment_count:,}; settlements: {settlements_total:,}")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Seed the local SentinelPay legacy warehouse.")
    parser.add_argument("--payments", type=int, default=1_000_000, help="Number of payment source rows to generate.")
    parser.add_argument("--customers", type=int, default=100_000)
    parser.add_argument("--merchants", type=int, default=500)
    parser.add_argument("--reset", action="store_true", help="Delete existing local seed data before loading.")
    args = parser.parse_args()

    if args.payments <= 0 or args.customers <= 0 or args.merchants <= 0:
        raise ValueError("All record counts must be positive.")

    dsn = os.getenv("SENTINELPAY_DB_DSN")
    if not dsn:
        raise ValueError("SENTINELPAY_DB_DSN is required. Copy .env.example to .env first.")

    random.seed(42)
    now = utc_now()
    with psycopg.connect(dsn) as connection:
        if args.reset:
            with connection.cursor() as cursor:
                cursor.execute("TRUNCATE settlements, payments, customers, merchants RESTART IDENTITY")
            connection.commit()
            print("Cleared existing legacy source data.")
        merchant_ids = generate_merchants(connection, args.merchants, now)
        customer_ids = generate_customers(connection, args.customers, now)
        connection.commit()
        generate_payments(connection, args.payments, merchant_ids, customer_ids, now)

    print("Legacy SentinelPay source generation completed.")


if __name__ == "__main__":
    main()
