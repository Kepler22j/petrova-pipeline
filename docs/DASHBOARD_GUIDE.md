# PETROVA Dashboard — Operations Guide (1st-Line)

A plain-language guide for the people who **watch this dashboard and respond first**.
Live: https://petrova-pipeline.streamlit.app

---

## 1. WHY this report exists
The pipeline moves **~400K equipment-sensor records/day** from source systems into the
warehouse. If a load fails silently, two bad things happen:
- **Data loss / stale dashboards** → the business makes decisions on old data.
- **Missed equipment alarms** → a failing asset (e.g. a compressor bearing) isn't caught early.

This report makes pipeline health and equipment anomalies **visible in one place**, so problems
are caught in minutes, not after the damage.

## 2. WHAT it shows (in one line)
> Is the data pipeline healthy, and is any equipment behaving abnormally?

Three things: **pipeline run status** (3 quality gates), **SPC anomaly alerts**, and **SLA/throughput**.

## 3. WHO it's for
| Audience | Uses it to… |
|---|---|
| **1st-line / on-call (you)** | Spot failures + critical alarms, do first response, escalate |
| **Data engineers** | Diagnose which gate failed and why |
| **Analysts / business** | Trust that today's numbers are fresh and validated |
| **Manager** | See SLA %, alert volume, overall health at a glance |

## 4. WHEN you MUST check it
- **Every morning, start of shift** — confirm last night's run = SUCCESS.
- **On any alert email** (`[CRITICAL]/[WARNING]` from the alert runner).
- **During an incident** — to see scope (which gate, how many records quarantined).
- **Before a stakeholder report goes out** — confirm data is fresh + gates green.

---

## 5. THE 3 PAGES — what to look at

### Page 1 — Pipeline Overview (health)
- **Records Processed** — should be ~400K/day. A drop = upstream/ingestion problem.
- **Gate Pass Rates** — Bronze → Silver → Gold. Lower % = more records failing that gate.
- **Duration Trend** — pipeline runtime. A sudden spike = performance issue.
- **Recent Runs table** — per-day status; `PARTIAL`/`FAIL` = investigate.

### Page 2 — Data Quality & SPC (equipment alarms)
- **OK / WARNING / CRITICAL** counts — the headline alarm numbers.
- **Severity Over Time** — red bars = CRITICAL days; a cluster = a developing fault.
- **Sensor Deep Dive** — pick a sensor to see its average + stddev (volatility) trend.

### Page 3 — Architecture (reference)
- The 9-layer design + 3-gate framework — context, not a daily-watch page.

---

## 6. DATA DICTIONARY (what each field means)

| Field | Meaning | Healthy | If not healthy |
|---|---|---|---|
| `records_processed` | rows ingested that day | ~380K–420K | <300K → check ingestion / source files |
| `bronze_gate` | schema/null validation result | PASS | FAIL → missing columns / nulls; check `quarantine_bronze` |
| `silver_gate` | business-rule + range validation | PASS | FAIL → >20% records failed rules |
| `gold_gate` | SPC severity check | PASS | FAIL → a CRITICAL alarm is present |
| `quarantined` | bad records routed aside (not loaded) | low / 0 | a spike = bad source batch |
| `duration_min` | pipeline runtime (minutes) | ~12–45 | sustained rise = performance issue |
| `alert_severity` | composite alarm level | OK | WARNING / CRITICAL → see below |
| `stability_level` | stddev band (STABLE/NORMAL/UNSTABLE) | STABLE/NORMAL | UNSTABLE = high variance |
| `signal_quality` | noise vs signal (coefficient of variation) | CLEAN | NOISY/VERY_NOISY = unreliable readings |
| `volatility_trend` | is variance rising day-over-day? | STEADY | INCREASING/DEGRADING = early warning |
| `has_outlier` | reading beyond 2σ | FALSE | TRUE = single abnormal spike |

---

## 7. SEVERITY → ACTION (1st-line playbook)

| Severity | What it means | Your action |
|---|---|---|
| 🟢 **OK** | within control limits | none — log normal |
| 🟡 **WARNING** | one signal tripped (outlier or rising variance) | note it; watch the sensor next run; no page |
| 🔴 **CRITICAL** | composite (UNSTABLE **and** noisy) | **page on-call + email**; record sensor/equipment; flag for inspection |

**Pipeline failure (gate FAIL):**
1. Open Page 1 → note which gate + records quarantined.
2. Check the alert email / `Logs/alert_*.log`.
3. Escalate to data engineering with: date, gate, quarantine count.
4. SLA protection means BI still serves the **cached Gold snapshot** — dashboards won't go dark.

---

## 8. 1st-LINE DAILY CHECKLIST (60 seconds)
- [ ] Last run = **SUCCESS**? (Page 1, Recent Runs)
- [ ] Records ~**400K**? (not a big drop)
- [ ] **CRITICAL = 0**? (Page 2) — if not, follow the playbook
- [ ] Any **DEGRADING** volatility trend? (early warning — flag it)
- [ ] Duration normal? (no big spike)

> Data source badge (sidebar): 🟢 Live (Snowflake) · 🟡 Demo (Gold CSV) · ⚪ Synthetic — tells you where the numbers come from.
