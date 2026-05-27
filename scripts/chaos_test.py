"""
PETROVA — Chaos Test Suite
Generates intentionally broken data files to test 3-Gate Validation resilience.

Tests:
    1. Schema drift (missing column)
    2. Wrong data types (string in numeric)
    3. All nulls (corrupt sensor batch)
    4. Duplicate records
    5. Late arriving data (3 days old)
    6. Empty file (0 records)
    7. Extreme outliers (values 100x normal)
    8. Mixed encoding / special characters

Usage:
    python scripts/chaos_test.py
    python scripts/chaos_test.py --landing-zone /mnt/landing/sensors/
    python scripts/chaos_test.py --test 1 3 5  # Run specific tests only
"""

import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime, timedelta

# Import the batch generator
from simulate_hourly_drops import generate_sensor_batch


def test_1_missing_column(landing_zone: str):
    """Schema drift: drop quality_flag column."""
    df = generate_sensor_batch(10_000)
    df = df.drop(columns=["quality_flag"])
    path = os.path.join(landing_zone, "chaos_01_missing_column.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 1: Missing column (quality_flag removed) -> {len(df):,} rows")
    return path


def test_2_wrong_types(landing_zone: str):
    """Type mismatch: string values in reading_value (numeric) column."""
    df = generate_sensor_batch(10_000)
    # Insert string values in first 500 rows
    df.loc[0:499, "reading_value"] = "INVALID_READING"
    # Force to object type so parquet accepts mixed types
    df["reading_value"] = df["reading_value"].astype(str)
    path = os.path.join(landing_zone, "chaos_02_wrong_types.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 2: Wrong types (500 string values in numeric column) -> {len(df):,} rows")
    return path


def test_3_all_nulls(landing_zone: str):
    """Corrupt batch: all reading_value and equipment_id are null."""
    df = generate_sensor_batch(10_000)
    df["reading_value"] = None
    df["equipment_id"] = None
    path = os.path.join(landing_zone, "chaos_03_all_nulls.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 3: All nulls (reading_value + equipment_id) -> {len(df):,} rows")
    return path


def test_4_duplicates(landing_zone: str):
    """Duplicate records: same records repeated 3x."""
    df = generate_sensor_batch(5_000)
    df_dupes = pd.concat([df, df, df], ignore_index=True)
    path = os.path.join(landing_zone, "chaos_04_duplicates.parquet")
    df_dupes.to_parquet(path, index=False)
    print(f"  Test 4: Duplicates (5K records x3 = {len(df_dupes):,} rows)")
    return path


def test_5_late_arriving(landing_zone: str):
    """Late arriving data: timestamps from 3 days ago."""
    df = generate_sensor_batch(10_000, base_time=datetime.now() - timedelta(days=3))
    path = os.path.join(landing_zone, "chaos_05_late_arriving.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 5: Late arriving (3 days old) -> {len(df):,} rows")
    return path


def test_6_empty_file(landing_zone: str):
    """Empty file: valid schema but 0 records."""
    df = generate_sensor_batch(1).head(0)  # Empty DataFrame with correct schema
    path = os.path.join(landing_zone, "chaos_06_empty_file.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 6: Empty file (0 rows, valid schema)")
    return path


def test_7_extreme_outliers(landing_zone: str):
    """Extreme outliers: values 100x above normal range."""
    df = generate_sensor_batch(10_000)
    # Set 200 records to extreme values
    df.loc[0:199, "reading_value"] = np.random.uniform(5000, 50000, 200).round(2)
    path = os.path.join(landing_zone, "chaos_07_extreme_outliers.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 7: Extreme outliers (200 values at 100x normal) -> {len(df):,} rows")
    return path


def test_8_negative_values(landing_zone: str):
    """Negative readings: physically impossible for sensors."""
    df = generate_sensor_batch(10_000)
    df.loc[0:999, "reading_value"] = np.random.uniform(-500, -1, 1000).round(2)
    path = os.path.join(landing_zone, "chaos_08_negative_values.parquet")
    df.to_parquet(path, index=False)
    print(f"  Test 8: Negative values (1000 impossible readings) -> {len(df):,} rows")
    return path


ALL_TESTS = {
    1: test_1_missing_column,
    2: test_2_wrong_types,
    3: test_3_all_nulls,
    4: test_4_duplicates,
    5: test_5_late_arriving,
    6: test_6_empty_file,
    7: test_7_extreme_outliers,
    8: test_8_negative_values,
}


def main():
    parser = argparse.ArgumentParser(description="PETROVA Chaos Test Suite")
    parser.add_argument("--landing-zone", type=str, default="data/landing/sensors/chaos/")
    parser.add_argument("--test", type=int, nargs="+", help="Specific test numbers to run")
    args = parser.parse_args()

    os.makedirs(args.landing_zone, exist_ok=True)

    print(f"PETROVA Chaos Test Suite")
    print(f"Landing zone: {args.landing_zone}")
    print("=" * 60)

    tests_to_run = args.test if args.test else sorted(ALL_TESTS.keys())

    results = []
    for test_num in tests_to_run:
        if test_num in ALL_TESTS:
            try:
                path = ALL_TESTS[test_num](args.landing_zone)
                results.append((test_num, "GENERATED", path))
            except Exception as e:
                results.append((test_num, "ERROR", str(e)))
                print(f"  Test {test_num}: ERROR - {e}")

    print("=" * 60)
    print(f"Generated {len(results)} chaos test files.")
    print("\nExpected 3-Gate behavior:")
    print("  Gate 1 (Bronze/Schema): Should catch tests 1, 2, 3, 6")
    print("  Gate 2 (Silver/Business): Should catch tests 4, 5, 8")
    print("  Gate 3 (Gold/Statistical): Should catch test 7")
    print("\nAll caught records should route to QUARANTINE, not Gold.")


if __name__ == "__main__":
    main()
