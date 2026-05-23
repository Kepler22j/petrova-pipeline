# PETROVA — Pipeline Weakness Analysis & Failover Strategy

**Author:** Jay Pechnarai | **Date:** 2026-05-21 | **Level:** Senior Data Engineer Interview Prep

---

## 1. The Problem: Semi-Waterfall Dependency

PETROVA uses a linear Medallion flow:

```
Bronze → Silver (Gate 1→2→3) → Gold → BI
```

This is sequential by design — each layer validates before promoting. But it creates a **cascade failure risk**:

| Layer Failure | Impact | Severity |
|--------------|--------|----------|
| Bronze fail (schema drift) | FULL STOP — no data enters pipeline | CRITICAL |
| Silver Gate 1 fail (null/type) | Downstream blocked — no cleaning | CRITICAL |
| Silver Gate 2 fail (business rules) | No Gold promotion | HIGH |
| Silver Gate 3 fail (cross-table) | Gold gets partial data | HIGH |
| Gold fail (SPC engine) | BI dashboards stale | HIGH |
| Compute fail (Spark/dbt) | Pipeline halted | CRITICAL |
| Orchestrator fail (Airflow) | System halt | CRITICAL |

**The honest admission:** In a pure waterfall, one gate failure blocks everything downstream. This is the #1 weakness interviewers will probe.

---

## 2. Mitigation Strategy: 3-Layer Failover

### Layer A: Partial Async Flow (`trigger_rule='all_done'`)

**Mechanism:** Airflow tasks use `trigger_rule='all_done'` instead of the default `'all_success'`. This means downstream tasks execute even if upstream tasks fail — but with quarantine routing.

```python
# Airflow DAG pattern
bronze_gate = PythonOperator(
    task_id='bronze_gate',
    python_callable=run_bronze_validation,
    retries=3,
    retry_delay=timedelta(minutes=5),
)

silver_gate = PythonOperator(
    task_id='silver_gate',
    python_callable=run_silver_validation,
    trigger_rule='all_done',    # runs even if bronze partially fails
)

quarantine_task = PythonOperator(
    task_id='quarantine_failed_records',
    python_callable=route_to_quarantine,
    trigger_rule='one_failed',  # only runs when upstream fails
)
```

**How it works:** If a batch of 400K IoT records contains 500 corrupt files, PySpark flags them with `quality_flag='FAIL'`, routes them to a quarantine Delta table, and continues processing the remaining 399,500 clean records through Silver → Gold.

**Pro:** Guarantees high availability — BI reporting doesn't stall because one sensor group has a hardware failure.

**Con:** Risk of "silent data loss" if alerting isn't tight. Quarantine table must have a monitoring task that alerts when row count spikes.

### Layer B: Fallback via Cached Gold (Zero-Copy Clone)

**Mechanism:** Before each Gold rebuild, Snowflake creates a Zero-Copy Clone as a snapshot. If the current Gold build fails, BI tools fall back to the cached snapshot.

```sql
-- Pre-build snapshot (runs as dbt pre-hook)
CREATE TABLE gold.fct_daily_sensor_kpi_cache
  CLONE gold.fct_daily_sensor_kpi;

-- If Gold build fails, BI reads from cache
-- Metadata flag: _is_cached = TRUE, _cache_timestamp = <snapshot time>
```

**Pro:** Phenomenal operational resilience. Downstream SLAs are protected even during pipeline failures.

**Con:** Introduces data staleness. Dashboards must visually flag when showing cached data — a banner like "Data as of 2026-05-21 02:00 AM (cached)" prevents business decisions on stale metrics.

### Layer C: Dual-Platform Resilience

**Mechanism:** PETROVA runs on both Databricks AND Snowflake. If one platform has an outage:

| Platform Down | Fallback | What Still Works |
|--------------|----------|-----------------|
| Databricks outage | Snowflake serves Gold tables | Power BI, Streamlit dashboards |
| Snowflake outage | Databricks Delta tables | Jupyter, Spark UI analytics |
| Both down | Cached Gold snapshot on local | Emergency read-only mode |

**Interview line:** "We chose dual-platform not for redundancy theater, but because Databricks excels at heavy PySpark transforms while Snowflake excels at analytical SQL queries. The resilience is a bonus."

---

## 3. Dependency Risk: Concrete Engineering Solutions

### Risk 1: Schema Drift (Bronze)
- **Problem:** Upstream sensor software update adds a new column → Auto Loader breaks
- **Solution:** Auto Loader `rescueDataPath` captures unknown columns instead of failing. Weekly schema reconciliation task reviews rescued columns.
- **Airflow:** `SchemaValidationOperator` → if drift detected → `EmailOperator('schema_drift_alert')`

### Risk 2: Logic Duplication (Silver)
- **Problem:** Same cleaning logic exists in both PySpark notebooks AND dbt models
- **Solution:** Single source of truth — dbt macros define all business rules. PySpark handles only physical transforms (cast, filter). No business logic in PySpark.
- **Pattern:** PySpark does Gate 1 (schema/type), dbt does Gates 2-3 (business rules, cross-table)

### Risk 3: High-Memory Shuffles (Gold)
- **Problem:** `collect_set()`, `stddev()`, window functions on 400K+ daily records cause driver OOM on skewed sensor data
- **Solution:** Pre-aggregate at Silver level (`fct_daily_sensor_kpi`), then run SPC on aggregated data (`fct_sensor_alerts`). Two-step reduces data volume 100x before heavy window functions.
- **Airflow:** Memory monitoring via `SparkSubmitOperator(conf={'spark.executor.memory': '4g'})`

### Risk 4: Checkpoint Corruption (Streaming)
- **Problem:** Structured Streaming checkpoint becomes corrupted → stream can't restart
- **Solution:** Checkpoint versioning with timestamp. On corruption, fall back to `trigger(once=True)` micro-batch mode via Airflow.
- **Airflow:** `ExternalTaskSensor(timeout=1800)` → if streaming task doesn't complete in 30 min → switch to batch mode

---

## 4. Alternative Architecture Trade-offs

### Option A: Snowflake + dbt Only (No Databricks)

| Aspect | Assessment |
|--------|-----------|
| **Pro** | Single control plane, unified SQL skill set, zero platform handoff latency |
| **Con** | Cost-inefficient for raw IoT parsing — regex, JSON flattening, array unnesting consume massive Snowflake credits vs. ephemeral Spark cluster |
| **When to choose** | Team is SQL-only, data volume < 50K records/day, no streaming requirement |

### Option B: Spark-First / dbt-Light (No Snowflake)

| Aspect | Assessment |
|--------|-----------|
| **Pro** | Maximum performance — PySpark handles all transforms, dbt only formats Gold views |
| **Con** | Steep barrier for analysts — all business logic locked in PySpark scripts, analysts can't self-serve |
| **When to choose** | Engineering-heavy team, no business analyst self-service requirement |

### Option C: Data Contract Driven (Schema Registry)

| Aspect | Assessment |
|--------|-----------|
| **Pro** | Exceptional governance — malformed data rejected at ingestion boundary before consuming compute |
| **Con** | Requires high organizational maturity — valid new columns rejected until schema registry updated manually |
| **When to choose** | Enterprise with dedicated data governance team, mature CI/CD for schema changes |

### PETROVA Choice: Hybrid (Option A + B)

**Why:** We use Databricks for heavy PySpark transforms (Gate 1: schema/type on raw Parquet) and Snowflake for analytical SQL (Gates 2-3 via dbt). This gives us the performance of Spark AND the accessibility of SQL.

**Interview line:** "I chose hybrid because the 400K records/day volume justifies Spark for ingestion, but our business analysts need dbt's SQL interface for self-service. It's not about having more tools — it's about using the right tool for each job."

---

## 5. Self-Assessment Scorecard

| Dimension | Score | Justification |
|-----------|-------|---------------|
| Design Quality | 9/10 | 10-layer architecture, 3-gate validation, SPC engine, traffic-light priority |
| Performance | 8/10 | Incremental models, ZORDER, surrogate keys, window functions |
| Resilience | 8/10 | Dual-platform, cached Gold, partial async, 3-layer failover |
| Maintainability | 7/10 | Cross-platform logic split (PySpark + dbt) adds cognitive load |
| Observability | 8/10 | PagerDuty + Airflow email + Streamlit dashboard + GitHub Actions CI |
| Security | 9/10 | 4-role RBAC, AES-256, TLS 1.2+, Key Vault, Unity Catalog |
| Cost Efficiency | 7/10 | Dual-platform = higher infrastructure cost, but justified by resilience |

**Overall: 8.0/10** — Production-grade with documented weaknesses and mitigation strategies.

---

## 6. Interview Playbook: "What Are Your Weaknesses?"

```
┌─────────────────────────────────────────────────────────┐
│           SENIOR INTERVIEW ANSWER BLUEPRINT             │
├─────────────────────────────────────────────────────────┤
│  1. CONTEXT:   "I built a 400K records/day pipeline     │
│                 across Databricks + Snowflake."         │
│  2. CANDOR:    "The main risk is cascade failure —      │
│                 if Bronze breaks, everything stops."     │
│  3. MITIGATION:"I handle this with partial async flow,  │
│                 quarantine routing, cached Gold          │
│                 snapshots, and dual-platform failover."  │
│  4. TRADE-OFF: "The cost is complexity — maintaining    │
│                 logic across two platforms requires      │
│                 clear ownership boundaries."             │
│  5. PROOF:     "Here's the code: trigger_rule,          │
│                 Zero-Copy Clone, PagerDuty escalation."  │
└─────────────────────────────────────────────────────────┘
```

**Why this works:**
- **Security:** You admit a real weakness, not a fake one
- **Maturity:** You show you've already solved it with engineering
- **Business impact:** You frame everything in terms of SLAs and BI availability
- **Code proof:** You can pull up the actual Airflow DAG to demonstrate

---

## 7. Quick-Reference: Notification Decision Tree

```
Record enters pipeline
  │
  ├─ Bronze Gate → PASS → continue
  │                FAIL → quarantine + EmailOperator('bronze_fail')
  │
  ├─ Silver Gate 1 → PASS/WARN → continue
  │                   FAIL → quarantine + EmailOperator('gate1_fail')
  │
  ├─ Silver Gate 2 → PASS/WARN → continue
  │                   FAIL → quarantine + EmailOperator('gate2_fail')
  │
  ├─ Silver Gate 3 → PASS/WARN → promote to Gold
  │                   FAIL → quarantine + EmailOperator('gate3_fail')
  │
  ├─ Gold SPC Engine
  │   ├─ OK       → Streamlit dashboard refresh
  │   ├─ WARNING  → EmailOperator + SlackWebhookOperator
  │   └─ CRITICAL → PagerDutyOperator (30-min escalation)
  │
  └─ Pipeline Timeout (SLA breach)
      └─ PagerDutyOperator('pipeline_timeout_critical')
```
