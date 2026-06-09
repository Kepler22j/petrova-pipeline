"""
PETROVA - Alert / Alarm Runner
==============================
Scans the Gold SPC table (fct_sensor_alerts) and FIRES an alarm when composite
severity hits CRITICAL. Prints a formatted alert, writes an alert log, and -
if SMTP env vars are set - emails the on-call address.

This is the demonstrable "we have alarm" piece for interviews:
    py scripts/send_alert.py          # demo: prints + logs the alarm (no creds needed)
    py scripts/send_alert.py --email  # also emails (needs ALERT_SMTP_* env vars)

Exit code: 2 = CRITICAL fired, 1 = WARNING only, 0 = all clear
(so it doubles as a cron / Airflow / CI gate).
"""
from __future__ import annotations

import argparse
import os
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

ALERTS_CSV = Path(__file__).resolve().parent.parent / "data" / "gold" / "fct_sensor_alerts.csv"
LOG_DIR = Path(__file__).resolve().parent.parent / "Logs"
ALERT_TO = os.environ.get("ALERT_TO", "pechnarai.jakapong@gmail.com")


def build_alert():
    df = pd.read_csv(ALERTS_CSV, parse_dates=["kpi_date"])
    latest = df["kpi_date"].max()
    today = df[df["kpi_date"] == latest]

    crit = today[today["alert_severity"] == "CRITICAL"]
    warn = today[today["alert_severity"] == "WARNING"]

    if len(crit):
        level, code = "CRITICAL", 2
    elif len(warn):
        level, code = "WARNING", 1
    else:
        level, code = "OK", 0

    lines = []
    lines.append("=" * 60)
    lines.append(f"PETROVA PIPELINE ALARM  [{level}]")
    lines.append(f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}  |  Data date: {latest:%Y-%m-%d}")
    lines.append("=" * 60)
    lines.append(f"CRITICAL: {len(crit)}   WARNING: {len(warn)}   "
                 f"(fleet sensors: {today['sensor_id'].nunique()})")
    lines.append("-" * 60)

    for _, r in crit.iterrows():
        lines.append(
            f"[CRITICAL] {r['equipment_name']} ({r['sensor_id']}) @ {r['process_area']}"
        )
        lines.append(
            f"           stddev={r['stddev_reading']}  stability={r['stability_level']}  "
            f"trend={r['volatility_trend']}  -> composite severity CRITICAL"
        )
        lines.append(
            f"           ACTION: page on-call (PagerDuty) + email; inspect for bearing wear."
        )
    if not len(crit) and len(warn):
        for _, r in warn.head(5).iterrows():
            lines.append(f"[WARNING]  {r['equipment_name']} ({r['sensor_id']}) - "
                         f"{r['stability_level']}, outlier={r['has_outlier']}")
    if level == "OK":
        lines.append("All sensors within control limits. No action required.")
    lines.append("=" * 60)

    subject = (f"[{level}] PETROVA - {len(crit)} critical / {len(warn)} warning "
               f"({latest:%Y-%m-%d})")
    return level, code, subject, "\n".join(lines)


def write_log(text: str):
    LOG_DIR.mkdir(exist_ok=True)
    path = LOG_DIR / f"alert_{datetime.now():%Y%m%d}.log"
    with open(path, "a", encoding="utf-8") as f:
        f.write(text + "\n")
    return path


def send_email(subject: str, body: str):
    host = os.environ.get("ALERT_SMTP_HOST")
    user = os.environ.get("ALERT_SMTP_USER")
    pw = os.environ.get("ALERT_SMTP_PASS")
    port = int(os.environ.get("ALERT_SMTP_PORT", "465"))
    if not (host and user and pw):
        print("  [email skipped] set ALERT_SMTP_HOST/USER/PASS to enable real sending.")
        return False
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ALERT_TO
    with smtplib.SMTP_SSL(host, port) as s:
        s.login(user, pw)
        s.sendmail(user, [ALERT_TO], msg.as_string())
    print(f"  [email sent] -> {ALERT_TO}")
    return True


def main():
    ap = argparse.ArgumentParser(description="PETROVA alert runner")
    ap.add_argument("--email", action="store_true", help="also send email (needs ALERT_SMTP_* env)")
    args = ap.parse_args()

    level, code, subject, body = build_alert()
    print(body)
    log = write_log(body)
    print(f"  [logged] {log}")
    print(f"  [recipient] {ALERT_TO}")
    if args.email:
        send_email(subject, body)
    sys.exit(code)


if __name__ == "__main__":
    main()
