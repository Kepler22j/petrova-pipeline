"""
PETROVA — Gold-Layer Demo Data Generator
════════════════════════════════════════
Generates realistic, *synthetic* offshore APM data for the portfolio
dashboard — no real production data involved.

It produces the four Gold tables exactly as the dbt models define them:
    • fct_daily_sensor_kpi   (daily per-sensor aggregates)
    • fct_sensor_alerts      (SPC engine: 6 categories from STDDEV + LAG + threshold)
    • fct_daily_revenue      (SAP order revenue by material group)
    • dim_equipment          (equipment dimension with lifecycle)

The SPC logic mirrors dbt/models/marts/fct_sensor_alerts.sql 1:1, so the
dashboard shows the same results the warehouse would.

A bearing-degradation event is injected on the K-201 gas compressor in the
final ~3 weeks so the dashboard tells a real story:
    STEADY → INCREASING volatility → UNSTABLE → CRITICAL composite alert.

Usage:
    py scripts/generate_gold_demo.py                 # 90 days, writes data/gold/
    py scripts/generate_gold_demo.py --days 180
    py scripts/generate_gold_demo.py --out data/gold --parquet
"""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

SEED = 42

# ── Realistic offshore equipment register (SAP-style FLOC tags) ──
# (equipment_id, equipment_name, equipment_type, process_area,
#  sensor_id, sensor_name, sensor_type, unit, baseline, normal_stddev)
EQUIPMENT = [
    ("EQ-1011", "P-101A Crude Transfer Pump",   "Centrifugal Pump",       "Separation Train",
     "S-VIB-101", "P-101A Vibration",   "VIBRATION",   "mm/s",  12.0,  1.2),
    ("EQ-2010", "K-201 Gas Compressor",         "Centrifugal Compressor", "Gas Compression",
     "S-VIB-201", "K-201 Vibration",    "VIBRATION",   "mm/s",  18.0,  2.0),
    ("EQ-3015", "E-301 Heat Exchanger",         "Shell & Tube HX",        "Separation Train",
     "S-TMP-301", "E-301 Outlet Temp",  "TEMPERATURE", "degC",  85.0,  3.5),
    ("EQ-4002", "V-401 Production Separator",   "3-Phase Separator",      "Wellhead Platform",
     "S-PRS-401", "V-401 Pressure",     "PRESSURE",    "bar",    6.5,  0.4),
    ("EQ-1022", "P-102B Water Injection Pump",  "Reciprocating Pump",     "Water Injection",
     "S-FLW-102", "P-102B Flow",        "FLOW",        "m3/h", 140.0,  3.5),
    ("EQ-5008", "C-501 Export Gas Compressor",  "Axial Compressor",       "Gas Export",
     "S-RPM-501", "C-501 Shaft Speed",  "RPM",         "rpm",  6500.0,  4.0),
]
# Note: fct_sensor_alerts uses fixed stddev thresholds (5/25). In production
# these would be per-sensor control limits; here baselines/stddevs are scaled
# so each healthy sensor sits in the STABLE/NORMAL band.

DEGRADING_SENSOR = "S-VIB-201"  # K-201 bearing wear story
MATERIAL_GROUPS = ["RAW", "SEMI", "FINISHED", "SPARE", "CONSUMABLE"]


def _sk(*parts) -> str:
    """dbt_utils.generate_surrogate_key — md5 of '-'-joined parts."""
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()


def build_sensor_kpi(days: int) -> pd.DataFrame:
    """fct_daily_sensor_kpi — daily per-sensor aggregates with an injected fault."""
    rng = np.random.default_rng(SEED)
    end = date(2025, 11, 30)
    dates = [end - timedelta(days=i) for i in range(days)][::-1]
    rows = []
    for (_eid, ename, _etype, area, sid, sname, _stype, _unit, base, nstd) in EQUIPMENT:
        for di, d in enumerate(dates):
            std = nstd
            mean = base
            # Inject bearing degradation on K-201 over the final 21 days:
            # sudden onset (DEGRADING) -> sustained rise (INCREASING/UNSTABLE)
            # -> CRITICAL in the final week.
            if sid == DEGRADING_SENSOR and di >= days - 21:
                ramp = (di - (days - 21)) / 21.0          # 0 -> 1
                std = nstd * (1 + ramp * 18)              # stddev grows up to ~19x (-> >25)
                mean = base * (1 + ramp * 0.12)            # slight upward drift
            total = int(rng.integers(8000, 16000))
            avg = float(rng.normal(mean, nstd * 0.15))
            stddev = float(abs(rng.normal(std, nstd * 0.10)))
            # min/max sit inside ~2 sigma normally; a genuine outlier event
            # (~6% of days) pushes max beyond 2 sigma -> trips has_outlier.
            k_hi = rng.uniform(1.6, 1.95)
            k_lo = rng.uniform(1.6, 1.95)
            if rng.random() < 0.06:
                k_hi = rng.uniform(2.2, 3.6)
            mn = round(avg - stddev * k_lo, 2)
            mx = round(avg + stddev * k_hi, 2)
            warn = int(total * rng.uniform(0.0, 0.04))
            rows.append({
                "kpi_sk": _sk(sid, d.isoformat()),
                "sensor_id": sid, "sensor_name": sname, "equipment_name": ename,
                "process_area": area, "kpi_date": d,
                "total_readings": total,
                "avg_reading": round(avg, 2),
                "min_reading": mn, "max_reading": mx,
                "stddev_reading": round(stddev, 2),
                "median_reading": round(avg, 2),
                "p95_reading": round(avg + stddev * 1.64, 2),
                "warn_count": warn,
            })
    return pd.DataFrame(rows)


def build_sensor_alerts(kpi: pd.DataFrame) -> pd.DataFrame:
    """fct_sensor_alerts — SPC engine. Mirrors fct_sensor_alerts.sql exactly."""
    df = kpi.sort_values(["sensor_id", "kpi_date"]).copy()
    g = df.groupby("sensor_id", group_keys=False)
    df["prev_stddev"] = g["stddev_reading"].shift(1)
    df["prev_avg"] = g["avg_reading"].shift(1)
    df["prev_max"] = g["max_reading"].shift(1)

    def classify(r):
        std, avg = r["stddev_reading"], r["avg_reading"]
        mn, mx = r["min_reading"], r["max_reading"]
        ps, pa, pm = r["prev_stddev"], r["prev_avg"], r["prev_max"]
        cv = round(std / abs(avg), 4) if avg not in (0, None) and not pd.isna(avg) else None

        # Alert 1 — stability
        if pd.isna(std):              stability = "INSUFFICIENT_DATA"
        elif std < 5:                 stability = "STABLE"
        elif std <= 25:               stability = "NORMAL"
        else:                         stability = "UNSTABLE"
        # Alert 2 — signal quality (coefficient of variation)
        if avg == 0 or pd.isna(avg):  signal = "INVALID"
        elif cv > 0.5:                signal = "VERY_NOISY"
        elif cv > 0.3:                signal = "NOISY"
        else:                         signal = "CLEAN"
        # Alert 3 — outlier (beyond 2 stddev)
        has_outlier = bool(mx > avg + 2 * std or mn < avg - 2 * std)
        # Alert 4 — range violation (outside avg ± 1 stddev)
        range_violated = bool(mn < avg - std or mx > avg + std)
        # Alert 5 — volatility trend (LAG on stddev)
        if pd.isna(ps):               vol = "NO_HISTORY"
        elif std > ps * 1.5:          vol = "DEGRADING"
        elif std > ps * 1.1:          vol = "INCREASING"
        elif std < ps * 0.9:          vol = "IMPROVING"
        else:                         vol = "STEADY"
        # Alert 6 — spike detection (LAG on max/avg)
        if pd.isna(pm):               spike = "NO_HISTORY"
        elif abs(mx - pm) > 2 * (std or 0):                      spike = "SPIKE"
        elif abs(avg - (pa if not pd.isna(pa) else avg)) > std:  spike = "SHIFT"
        else:                         spike = "NORMAL"
        # Composite severity
        if std > 25 and cv is not None and cv > 0.3:
            severity = "CRITICAL"
        elif std > 25 or has_outlier or (not pd.isna(ps) and std > ps * 1.5):
            severity = "WARNING"
        else:
            severity = "OK"

        return pd.Series({
            "alert_sk": _sk(r["sensor_id"], r["kpi_date"].isoformat()),
            "stability_level": stability, "signal_quality": signal,
            "coefficient_of_variation": cv, "has_outlier": has_outlier,
            "expected_lower": round(avg - std, 2), "expected_upper": round(avg + std, 2),
            "range_violated": range_violated, "volatility_trend": vol,
            "spike_status": spike, "alert_severity": severity,
        })

    out = df.join(df.apply(classify, axis=1))
    cols = ["alert_sk", "sensor_id", "sensor_name", "equipment_name", "process_area",
            "kpi_date", "avg_reading", "stddev_reading", "min_reading", "max_reading",
            "total_readings", "stability_level", "signal_quality",
            "coefficient_of_variation", "has_outlier", "expected_lower", "expected_upper",
            "range_violated", "prev_stddev", "volatility_trend", "spike_status",
            "alert_severity"]
    return out[cols].reset_index(drop=True)


def build_daily_revenue(days: int) -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 1)
    end = date(2025, 11, 30)
    dates = [end - timedelta(days=i) for i in range(days)][::-1]
    rows = []
    for d in dates:
        for mg in MATERIAL_GROUPS:
            orders = int(rng.integers(5, 60))
            qty = float(round(rng.uniform(50, 5000), 2))
            rev = float(round(qty * rng.uniform(20, 400), 2))
            rows.append({
                "revenue_sk": _sk(d.isoformat(), mg),
                "revenue_date": d, "material_group": mg,
                "order_count": orders, "total_quantity": qty,
                "total_revenue": rev,
                "avg_order_value": round(rev / orders, 2),
            })
    return pd.DataFrame(rows)


def build_dim_equipment() -> pd.DataFrame:
    rng = np.random.default_rng(SEED + 2)
    today = date(2025, 11, 30)
    rows = []
    for (eid, ename, etype, area, *_rest) in EQUIPMENT:
        install = today - timedelta(days=int(rng.integers(400, 4000)))
        dsi = (today - install).days
        stage = ("NEW" if dsi < 365 else "MATURE" if dsi < 2500 else "END_OF_LIFE")
        rows.append({
            "equipment_sk": _sk(eid),
            "equipment_id": eid, "equipment_name": ename, "equipment_type": etype,
            "location": area, "install_date": install,
            "status": "ACTIVE", "days_since_install": dsi, "lifecycle_stage": stage,
        })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="PETROVA Gold-layer demo data generator")
    ap.add_argument("--days", type=int, default=90, help="Days of history (default 90)")
    ap.add_argument("--out", type=str, default="data/gold", help="Output directory")
    ap.add_argument("--parquet", action="store_true", help="Also write Parquet")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"PETROVA Gold demo generator -- {args.days} days -> {args.out}")
    print("=" * 64)

    kpi = build_sensor_kpi(args.days)
    alerts = build_sensor_alerts(kpi)
    revenue = build_daily_revenue(args.days)
    equipment = build_dim_equipment()

    tables = {
        "fct_daily_sensor_kpi": kpi,
        "fct_sensor_alerts": alerts,
        "fct_daily_revenue": revenue,
        "dim_equipment": equipment,
    }
    for name, df in tables.items():
        df.to_csv(os.path.join(args.out, f"{name}.csv"), index=False)
        if args.parquet:
            df.to_parquet(os.path.join(args.out, f"{name}.parquet"), index=False)
        print(f"  {name:<22} {len(df):>5} rows -> {name}.csv")

    sev = alerts["alert_severity"].value_counts().to_dict()
    fleet_daily = int(kpi.groupby('kpi_date')['total_readings'].sum().mean())
    print("-" * 64)
    print(f"  Alert severity mix : {sev}")
    print(f"  Showcased sensors  : {kpi['sensor_id'].nunique()} "
          f"(fleet target 50 units ~ 400K+ readings/day)")
    print(f"  Avg readings/day   : {fleet_daily:,} across showcased sensors")
    crit = alerts[alerts.alert_severity == 'CRITICAL']
    if len(crit):
        first = crit.sort_values('kpi_date').iloc[0]
        print(f"  First CRITICAL     : {first['equipment_name']} on {first['kpi_date']}")
    print("Done.")


if __name__ == "__main__":
    main()
