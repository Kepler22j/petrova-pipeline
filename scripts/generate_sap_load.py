"""
PETROVA — SAP Large File Load Generator
Generates 10-20 GB synthetic SAP data for performance/load testing.

Designed to run on Databricks (PySpark) for realistic volume testing.
Can also run locally with pandas for smaller datasets.

Usage (Databricks notebook):
    %run ../scripts/generate_sap_load

    # Generate 50M rows (~10-15 GB)
    df = generate_sap_orders(spark, num_rows=50_000_000)
    df.write.parquet("/mnt/landing/sap_orders_full/", mode="overwrite")

Usage (Local/pandas for smaller test):
    python scripts/generate_sap_load.py --rows 1000000 --output data/landing/sap/
"""

import os
import argparse
from datetime import datetime


# ============================================================
# PySpark Version (for Databricks — handles 10-20 GB)
# ============================================================

def generate_sap_orders_spark(spark, num_rows: int = 50_000_000):
    """Generate synthetic SAP orders using PySpark.

    50M rows ≈ 10-15 GB in Parquet format.
    Simulates SAP BODS extract with realistic distributions.

    Args:
        spark: SparkSession
        num_rows: Number of records to generate

    Returns:
        PySpark DataFrame
    """
    from pyspark.sql import functions as F

    df = (spark.range(num_rows)
        .withColumn("order_id",
            F.concat(F.lit("ORD-"), F.lpad(F.col("id").cast("string"), 10, "0")))
        .withColumn("material_id",
            F.concat(F.lit("MAT-"), F.lpad(
                (F.col("id") % 5000).cast("string"), 5, "0")))
        .withColumn("vendor_id",
            F.concat(F.lit("VND-"), F.lpad(
                (F.col("id") % 200).cast("string"), 4, "0")))
        .withColumn("customer_id",
            F.concat(F.lit("CUST-"), F.lpad(
                (F.col("id") % 1000).cast("string"), 5, "0")))
        .withColumn("plant_code",
            F.concat(F.lit("PLT-"),
                F.array(F.lit("BKK"), F.lit("SGP"), F.lit("HKG"),
                        F.lit("TYO"), F.lit("SYD"))
                .getItem((F.col("id") % 5).cast("int"))))
        .withColumn("quantity",
            (F.rand() * 1000 + 1).cast("int"))
        .withColumn("unit_price",
            F.round(F.rand() * 500 + 0.01, 2))
        .withColumn("total_amount",
            F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("currency",
            F.array(F.lit("USD"), F.lit("THB"), F.lit("SGD"), F.lit("JPY"))
            .getItem((F.col("id") % 4).cast("int")))
        .withColumn("order_date",
            F.date_add(F.lit("2024-01-01"), (F.rand() * 545).cast("int")))
        .withColumn("delivery_date",
            F.date_add(F.col("order_date"), (F.rand() * 30 + 1).cast("int")))
        .withColumn("status",
            F.array(F.lit("ACTIVE"), F.lit("INACTIVE"),
                    F.lit("PENDING"), F.lit("CANCELLED"), F.lit("COMPLETED"))
            .getItem((F.rand() * 5).cast("int")))
        .withColumn("created_by",
            F.concat(F.lit("SAP_USER_"),
                (F.col("id") % 50).cast("string")))
        .withColumn("source_system", F.lit("SAP_BODS"))
        .withColumn("extraction_timestamp", F.current_timestamp())
        .drop("id")
    )

    return df


def generate_sap_materials_spark(spark, num_rows: int = 5_000):
    """Generate synthetic SAP materials master data."""
    from pyspark.sql import functions as F

    df = (spark.range(num_rows)
        .withColumn("material_id",
            F.concat(F.lit("MAT-"), F.lpad(F.col("id").cast("string"), 5, "0")))
        .withColumn("material_name",
            F.concat(F.lit("Material "), F.col("id").cast("string")))
        .withColumn("material_group",
            F.array(F.lit("RAW"), F.lit("SEMI"), F.lit("FINISHED"),
                    F.lit("SPARE"), F.lit("CONSUMABLE"))
            .getItem((F.col("id") % 5).cast("int")))
        .withColumn("unit_of_measure",
            F.array(F.lit("EA"), F.lit("KG"), F.lit("L"), F.lit("M"))
            .getItem((F.col("id") % 4).cast("int")))
        .withColumn("standard_cost",
            F.round(F.rand() * 1000 + 1, 2))
        .withColumn("is_active", F.lit(True))
        .withColumn("source_system", F.lit("SAP_BODS"))
        .drop("id")
    )

    return df


def generate_sensor_readings_spark(spark, num_rows: int = 10_000_000):
    """Generate synthetic IoT sensor readings at scale.

    10M rows ≈ 2-3 GB. Use for streaming simulation.
    """
    from pyspark.sql import functions as F

    equipment_count = 50
    sensor_types = ["VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW"]

    df = (spark.range(num_rows)
        .withColumn("equipment_id",
            F.concat(F.lit("EQ-"), F.lpad(
                (F.col("id") % equipment_count + 1).cast("string"), 4, "0")))
        .withColumn("sensor_type",
            F.array(*[F.lit(s) for s in sensor_types])
            .getItem((F.col("id") % 4).cast("int")))
        .withColumn("reading_value",
            F.when(F.col("sensor_type") == "VIBRATION",
                   F.round(F.randn() * 3.0 + 12.5, 2))
            .when(F.col("sensor_type") == "TEMPERATURE",
                  F.round(F.randn() * 10.0 + 75.0, 2))
            .when(F.col("sensor_type") == "PRESSURE",
                  F.round(F.randn() * 0.8 + 4.5, 2))
            .otherwise(F.round(F.randn() * 25.0 + 120.0, 2)))
        .withColumn("unit",
            F.when(F.col("sensor_type") == "VIBRATION", F.lit("mm/s"))
            .when(F.col("sensor_type") == "TEMPERATURE", F.lit("C"))
            .when(F.col("sensor_type") == "PRESSURE", F.lit("bar"))
            .otherwise(F.lit("m3/h")))
        .withColumn("reading_timestamp",
            F.from_unixtime(
                F.unix_timestamp(F.lit("2025-01-01")) +
                (F.col("id") * 3).cast("long")))  # 3-second intervals
        .withColumn("quality_flag",
            F.when(F.rand() < 0.02, F.lit("FAIL"))
            .when(F.rand() < 0.05, F.lit("WARN"))
            .otherwise(F.lit("PASS")))
        .withColumn("source_system", F.lit("IOT_SENSORS"))
        .drop("id")
    )

    return df


# ============================================================
# Pandas Version (for local testing — smaller datasets)
# ============================================================

def generate_sap_orders_pandas(num_rows: int = 1_000_000):
    """Generate SAP orders using pandas (local, max ~5M rows)."""
    import pandas as pd
    import numpy as np

    print(f"Generating {num_rows:,} SAP order records with pandas...")

    df = pd.DataFrame({
        "order_id": [f"ORD-{i:010d}" for i in range(num_rows)],
        "material_id": [f"MAT-{i % 5000:05d}" for i in range(num_rows)],
        "vendor_id": [f"VND-{i % 200:04d}" for i in range(num_rows)],
        "customer_id": [f"CUST-{i % 1000:05d}" for i in range(num_rows)],
        "quantity": np.random.randint(1, 1000, num_rows),
        "unit_price": np.round(np.random.uniform(0.01, 500, num_rows), 2),
        "order_date": pd.date_range("2024-01-01", periods=num_rows, freq="s")
                        .to_series().sample(num_rows, replace=True).reset_index(drop=True),
        "status": np.random.choice(
            ["ACTIVE", "INACTIVE", "PENDING", "CANCELLED", "COMPLETED"], num_rows),
        "source_system": "SAP_BODS",
    })
    df["total_amount"] = np.round(df["quantity"] * df["unit_price"], 2)

    return df


def main():
    parser = argparse.ArgumentParser(description="PETROVA SAP Load Generator")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows (default: 1M)")
    parser.add_argument("--output", type=str, default="data/landing/sap/", help="Output directory")
    parser.add_argument("--table", type=str, default="orders",
                        choices=["orders", "materials", "sensors"],
                        help="Which table to generate")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"PETROVA SAP Load Generator")
    print(f"Table: {args.table} | Rows: {args.rows:,} | Output: {args.output}")
    print("=" * 60)

    if args.table == "orders":
        df = generate_sap_orders_pandas(args.rows)
    else:
        print("For materials and sensors at scale, use PySpark on Databricks.")
        print("See generate_sap_orders_spark() / generate_sensor_readings_spark()")
        return

    fname = f"sap_{args.table}_{args.rows}_{datetime.now():%Y%m%d}.parquet"
    filepath = os.path.join(args.output, fname)
    df.to_parquet(filepath, index=False)

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Written: {filepath} ({size_mb:.1f} MB, {len(df):,} rows)")
    print("\nFor 10-20 GB load tests, run on Databricks:")
    print("  df = generate_sap_orders_spark(spark, 50_000_000)")
    print("  df.write.parquet('/mnt/landing/sap_orders_full/')")


if __name__ == "__main__":
    main()
