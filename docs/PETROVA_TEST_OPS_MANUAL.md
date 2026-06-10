# PETROVA - Test & Operations Manual

Full architecture, huge-data mock generation, weekly batch jobs, and test scripts
(with config + expected results), incl. a data-loss test. Tools: dbt - Databricks - Snowflake.
All steps have a non-CLI (UI) path.

---

## 1. Project Overview
PETROVA is a hybrid-cloud data platform that ingests 10 heterogeneous sources into a
medallion lakehouse (Bronze -> Silver -> Gold), validates with 3 gates, materializes
business KPIs + SPC alerts in Gold, and serves Power BI + Streamlit. Orchestrated by
Airflow with SLA-resilient graceful degradation.

## 2. Full Architecture (end-to-end)
```
10 SOURCES (static + dynamic)
  IoT - SCADA/OPC-UA - Logs - SAP/ERP - CDC(Debezium) - CMMS - Weather - Reference - Clickstream - SFTP
        | Kafka/Spark - ADF - Snowpipe - Auto Loader - Airflow
        v
 BRONZE (raw, immutable)  --GATE 1 (schema/type/null)-->
 SILVER (dedupe, cleanse, schema, standardise, validate, SCD2/CDC, enrich)  --GATE 2 (business)-->
 GOLD   (aggregate KPIs, SPC alerts, immutable)  --GATE 3 (statistical/SPC)-->
 CONSUMPTION (Power BI - Streamlit - PagerDuty/Email)
 Orchestration: Airflow   Monitoring: ELK + Prometheus/Grafana   Resilience: all_done + Zero-Copy Clone
```

---

## 3. Mock Data Generation - HUGE data for testing
Goal: generate millions of rows to load-test the pipeline. Pick the engine you're testing.

### 3a. Snowflake (Snowsight UI -> Worksheet) - fastest for huge volume
```sql
-- Generate 10,000,000 synthetic sensor rows into Bronze
INSERT INTO bronze_sensor (equipment_id, ts, temperature, pressure, vibration)
SELECT
  'EQ-' || UNIFORM(1000, 1100, RANDOM())            AS equipment_id,
  DATEADD(second, SEQ4(), '2026-01-01'::timestamp)  AS ts,
  ROUND(UNIFORM(20, 120, RANDOM()), 2)              AS temperature,
  ROUND(UNIFORM(0, 5000, RANDOM()), 1)              AS pressure,
  ROUND(UNIFORM(0, 5, RANDOM()), 2)                 AS vibration
FROM TABLE(GENERATOR(ROWCOUNT => 10000000));
```
Expected: ~10M rows in seconds (warehouse scales). Verify: `SELECT COUNT(*) FROM bronze_sensor;` -> 10000000.

### 3b. Databricks (Notebook UI) - PySpark
```python
from pyspark.sql import functions as F
df = (spark.range(0, 10_000_000)
        .withColumn("equipment_id", F.concat(F.lit("EQ-"), (F.col("id") % 100 + 1000).cast("string")))
        .withColumn("ts", F.expr("timestampadd(SECOND, id, timestamp'2026-01-01')"))
        .withColumn("temperature", F.round(F.rand()*100 + 20, 2))
        .withColumn("pressure", F.round(F.rand()*5000, 1)))
df.write.mode("overwrite").saveAsTable("bronze_sensor")
print(df.count())   # expect 10000000
```

### 3c. Python (local) - faker / numpy -> Parquet (your existing generator)
- Reuse `scripts/generate_gold_demo.py` pattern; scale ROWCOUNT up; write Parquet to the raw zone.
- Inject a known fault (e.g., K-201 bearing degradation) so anomaly tests have a target.

**Inject test edge-cases on purpose:** duplicates (same key+ts), nulls, out-of-range pressure (>5000), late timestamps - so the Silver/gate tests have something to catch.

---

## 4. Weekly Batch Job (non-CLI / UI)
Schedule the full Bronze->Silver->Gold run weekly (e.g., Sunday 02:00). Pick your orchestrator:

| Tool | UI path | Schedule (cron) |
|---|---|---|
| **dbt Cloud** | Deploy -> Jobs -> + Create job -> command `dbt build` -> Triggers -> Schedule | `0 2 * * 0` |
| **Databricks Workflows** | Workflows -> Create job -> add tasks (ingest, transform) -> Schedule -> Weekly | `0 2 * * 0` |
| **Snowflake Task** (Snowsight) | Data -> create Task on the merge proc -> set schedule | `USING CRON 0 2 * * 0 UTC` |
| **Airflow** | DAG with `schedule="0 2 * * 0"` -> enable in UI | weekly |

Expected: job runs every Sunday 02:00, all tasks green, Gold refreshed, run logged.

---

## 5. Test Plan (test-script style)
| # | Test | Action / Command | Config | Expected Result |
|---|---|---|---|---|
| T1 | **Volume / load** | generate 10M rows (S3a) -> run pipeline | warehouse = M, autoscale | pipeline completes; no OOM; runtime within SLA |
| T2 | **Data quality (3-gate)** | run `dbt build` (tests on) | gates: schema/business/SPC | all tests PASS; bad rows quarantined |
| T3 | **Idempotency / re-run** | run the weekly job **twice** | same input | row counts identical after 2nd run (no dupes) |
| T4 | **Deduplication** | inject 5% dup keys -> run Silver | ROW_NUMBER dedupe | dup rows removed; count = distinct keys |
| T5 | **Data loss / reconciliation** | compare counts across layers | see Section 6 | no unexplained loss (Section 6 rule) |
| T6 | **SLA / failure / graceful** | force a Gold rebuild failure | `trigger_rule=all_done` + clone | dashboard serves last-good clone; alert fires |

---

## 6. Data Loss Test (detailed) - the critical one
**Goal:** prove no rows are silently lost from source -> Bronze -> Silver -> Gold.

### 6a. Row-count reconciliation (Snowsight / dbt test)
```sql
-- Source vs Bronze (should match exactly; Bronze is raw)
SELECT
  (SELECT COUNT(*) FROM raw_source_sensor) AS src_cnt,
  (SELECT COUNT(*) FROM bronze_sensor)     AS bronze_cnt;
-- EXPECT: src_cnt = bronze_cnt   (Bronze = raw, no drops)
```
```sql
-- Bronze vs Silver (loss is EXPLAINED only by dedup + quarantine)
SELECT
  (SELECT COUNT(*) FROM bronze_sensor)                              AS bronze_cnt,
  (SELECT COUNT(*) FROM silver_sensor)                              AS silver_cnt,
  (SELECT COUNT(*) FROM quarantine_silver)                          AS quarantined,
  (SELECT COUNT(*) FROM bronze_sensor) - (SELECT COUNT(DISTINCT equipment_id||ts FROM bronze_sensor)) AS dupes_removed;
-- EXPECT: bronze_cnt = silver_cnt + quarantined + dupes_removed   (every row accounted for)
```

### 6b. Key completeness (no key vanished)
```sql
SELECT b.equipment_id
FROM   (SELECT DISTINCT equipment_id FROM bronze_sensor) b
LEFT JOIN (SELECT DISTINCT equipment_id FROM gold_sensor_kpi) g
  ON b.equipment_id = g.equipment_id
WHERE  g.equipment_id IS NULL;
-- EXPECT: 0 rows  (every valid equipment reached Gold)
```

### 6c. Checksum (values intact, not just counts)
```sql
SELECT MD5(LISTAGG(equipment_id || pressure, ',') WITHIN GROUP (ORDER BY ts)) AS hash
FROM silver_sensor WHERE ts::date = '2026-01-01';
-- EXPECT: stable hash across re-runs (idempotent, no corruption)
```

### 6d. Pass/Fail rule
> **PASS** if: `src = bronze` AND `bronze = silver + quarantined + dupes_removed` AND key-completeness = 0 missing AND checksum stable on re-run.
> **FAIL** -> investigate in ELK by `correlation_id`, check the failing gate, fix, re-run.

---

## 7. Sign-off checklist
- [ ] 10M mock rows generated + edge-cases injected
- [ ] Weekly job scheduled + ran green
- [ ] T1-T6 all pass
- [ ] Data-loss reconciliation (Section 6) = PASS
- [ ] Alerts fired to 1st-line on forced failure
- [ ] Dashboard stayed live during rebuild failure (graceful degradation)
