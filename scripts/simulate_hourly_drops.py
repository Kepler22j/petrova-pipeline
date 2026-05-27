"""
PETROVA — Hourly Sensor File Drop Simulator
Generates synthetic IoT sensor data and drops Parquet files to landing zone.

Usage:
    # Single drop (400K records)
    python scripts/simulate_hourly_drops.py

    # Custom size
    python scripts/simulate_hourly_drops.py --records 100000

    # Continuous hourly drops (run with cron or Airflow)
    python scripts/simulate_hourly_drops.py --continuous --interval 3600
"""

import pandas as pd
import numpy as np
import os
import sys
import time
import argparse
from datetime import datetime, timedelta


def generate_sensor_batch(n_records: int = 400_000, base_time: datetime = None) -> pd.DataFrame:
    """Generate one batch of IoT sensor readings.

    Simulates 50 equipment units with 4 sensor types.
    Includes realistic distributions: 95% PASS, 3% WARN, 2% FAIL.
    """
    if base_time is None:
        base_time = datetime.now()

    equipment_ids = [f"EQ-{i:04d}" for i in range(1, 51)]
    sensor_types = ["VIBRATION", "TEMPERATURE", "PRESSURE", "FLOW"]
    units_map = {
        "VIBRATION": "mm/s",
        "TEMPERATURE": "C",
        "PRESSURE": "bar",
        "FLOW": "m3/h",
    }

    # Generate base data
    sensors = np.random.choice(sensor_types, n_records)
    readings = pd.DataFrame({
        "equipment_id": np.random.choice(equipment_ids, n_records),
        "sensor_type": sensors,
        "reading_value": np.where(
            sensors == "VIBRATION", np.random.normal(12.5, 3.0, n_records),
            np.where(
                sensors == "TEMPERATURE", np.random.normal(75.0, 10.0, n_records),
                np.where(
                    sensors == "PRESSURE", np.random.normal(4.5, 0.8, n_records),
                    np.random.normal(120.0, 25.0, n_records)  # FLOW
                )
            )
        ).round(2),
        "unit": [units_map[s] for s in sensors],
        "reading_timestamp": [
            base_time + timedelta(milliseconds=int(i * (3600000 / n_records)))
            for i in range(n_records)
        ],
        "quality_flag": np.random.choice(
            ["PASS", "PASS", "PASS", "PASS", "PASS",  # 95% PASS
             "PASS", "PASS", "PASS", "PASS", "PASS",
             "PASS", "PASS", "PASS", "PASS", "PASS",
             "PASS", "PASS", "PASS", "PASS", "WARN",  # 3% WARN
             "FAIL"],                                   # 2% FAIL
            n_records
        ),
    })

    # Add source metadata
    readings["source_file"] = f"sensor_{base_time:%Y%m%d_%H%M}.parquet"
    readings["ingestion_timestamp"] = datetime.now()

    return readings


def drop_file(df: pd.DataFrame, landing_zone: str, filename: str):
    """Write batch to Parquet in the landing zone."""
    os.makedirs(landing_zone, exist_ok=True)
    filepath = os.path.join(landing_zone, filename)
    df.to_parquet(filepath, index=False, engine="pyarrow")
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"[{datetime.now():%H:%M:%S}] Dropped {len(df):,} records -> {filename} ({size_mb:.1f} MB)")
    return filepath


def main():
    parser = argparse.ArgumentParser(description="PETROVA Sensor File Drop Simulator")
    parser.add_argument("--records", type=int, default=400_000, help="Records per batch (default: 400K)")
    parser.add_argument("--landing-zone", type=str, default="data/landing/sensors/", help="Landing zone path")
    parser.add_argument("--continuous", action="store_true", help="Run continuously with interval")
    parser.add_argument("--interval", type=int, default=3600, help="Seconds between drops (default: 3600)")
    parser.add_argument("--batches", type=int, default=1, help="Number of batches to generate")
    args = parser.parse_args()

    print(f"PETROVA Sensor Simulator — {args.records:,} records/batch")
    print(f"Landing zone: {args.landing_zone}")
    print("=" * 60)

    if args.continuous:
        batch_num = 0
        while True:
            batch_num += 1
            base_time = datetime.now()
            df = generate_sensor_batch(args.records, base_time)
            fname = f"sensor_{base_time:%Y%m%d_%H%M}.parquet"
            drop_file(df, args.landing_zone, fname)
            print(f"  Batch #{batch_num} complete. Next drop in {args.interval}s...")
            time.sleep(args.interval)
    else:
        for i in range(args.batches):
            base_time = datetime.now() - timedelta(hours=args.batches - i - 1)
            df = generate_sensor_batch(args.records, base_time)
            fname = f"sensor_{base_time:%Y%m%d_%H%M}.parquet"
            drop_file(df, args.landing_zone, fname)

    print("Done.")


if __name__ == "__main__":
    main()
