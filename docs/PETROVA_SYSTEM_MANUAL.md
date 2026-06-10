# PETROVA - System Manual
### Architecture · Data Architecture · Application Design · Operations
_Author: Jakapong Pechnarai (Jay) · Synthetic portfolio platform (no production/company data)_

---

## 1. Executive Summary
PETROVA is a hybrid-cloud, production-grade data platform for Industrial IoT (offshore-rig
style telemetry). It ingests 10 heterogeneous sources into a medallion lakehouse
(Bronze -> Silver -> Gold), validates with 3 quality gates, materializes business-ready
data products (KPIs, SPC anomaly alerts), and serves Power BI + Streamlit. It is
orchestrated by Airflow and observed with ELK + Prometheus/Grafana, with SLA-resilient
graceful degradation. Stack: Snowflake, Databricks, dbt, Airflow, PySpark, Delta Lake.

---

## 2. System Architecture

### 2.1 Layered view
```
L0 Sources      IoT · SCADA/OPC-UA · App Logs · SAP/ERP · CDC · CMMS · Weather · Reference · Clickstream · SFTP
L1 Ingestion    Kafka/Spark · ADF · Snowpipe · Auto Loader · Airflow
L2 Medallion    BRONZE (raw) -> SILVER (clean+enrich) -> GOLD (materialized products)
L3 Compute      Databricks (Delta/Unity Catalog) · Snowflake (DW) · PySpark
L4 Orchestration Airflow (DAGs, SLA, retries)
L5 Analytics    Power BI · Streamlit · dbt docs · Jupyter
L6 Monitoring   ELK (logs) · Prometheus/Grafana (metrics) · PagerDuty (alerts)
L7 Security     RBAC (4 roles) · Unity Catalog · Key Vault · TLS · Time Travel
L8 Local Dev    Docker Compose (Postgres, Airflow, Spark, MinIO)
L9 Query Perf   Auto-suspend WH · Z-Order · materialized views · caching
```

### 2.2 End-to-end flow
```
SOURCES -> INGEST -> [GATE1] -> BRONZE -> SILVER -> [GATE2] -> GOLD -> [GATE3] -> CONSUMPTION
   Airflow orchestrates · ELK+Prometheus observe · clone+all_done = resilience
```

### 2.3 Key components
| Component | Role |
|---|---|
| Ingestion engines | Kafka/Spark (stream), ADF/Snowpipe/Auto Loader (batch/continuous) |
| Bronze | raw, immutable, append-only, source-aligned |
| Silver | cleaned, validated, enriched (10 logics) |
| Gold | aggregated KPIs + SPC alerts (immutable marts) |
| Orchestrator | Airflow DAG with 3 gates + SLA callbacks |
| Observability | ELK + Prometheus/Grafana + PagerDuty |

---

## 3. Data Architecture

### 3.1 Medallion
- **Bronze** = raw landing (no logic). **Silver** = trusted (clean+enrich). **Gold** = valuable (materialized products).

### 3.2 Data model
- **Facts** (append-only): `fct_sensor_readings`, `fct_daily_sensor_kpi`, `fct_sensor_alerts`
- **Dimensions** (SCD Type-2): `dim_equipment`, `dim_vendor`, `dim_customer`
- **Keys:** business key + `row_hash` (MD5) · `is_current`, `valid_from/valid_to` for history

### 3.3 Ingestion lanes (static vs dynamic)
| Lane | Examples | Tool | Pattern |
|---|---|---|---|
| Static (reference) | Equipment Master, SAP, reference | ADF/SAP BODS/dbt seeds | full-refresh or SCD2 |
| Dynamic (signal) | IoT, SCADA, CDC, clickstream | Kafka/Snowpipe/Auto Loader/Debezium | append + watermark |

*(10-source detail: see `STATIC_VS_DYNAMIC_INGESTION.md`.)*

### 3.4 Schema & naming
`petrova_bronze` · `petrova_silver` · `petrova_gold` (or domain-aligned: ops, finance).

---

## 4. Application Design

### 4.1 Component design
| Module | Responsibility | Interface |
|---|---|---|
| Ingestion | land raw to Bronze | files/streams -> Delta/Snowflake |
| Transformation | clean+enrich (Silver), aggregate (Gold) | dbt models / PySpark |
| Validation | 3 gates (schema/business/SPC) | dbt tests / Great Expectations |
| Orchestration | schedule + dependency + SLA | Airflow DAG |
| Serving | dashboards + alerts | Power BI / Streamlit / PagerDuty |

### 4.2 Design principles
- **Immutability** (Bronze + Gold) · **Idempotency** (MERGE/UPSERT, safe re-runs)
- **Exactly-once** (checkpoint + idempotent writes) · **Watermarking** (late data)
- **SLA-resilience** (`trigger_rule=all_done` + Zero-Copy Clone -> dashboards never go dark)
- **Separation of elasticity** (K8s/pods scale orchestration; Snowflake/Databricks scale compute)

### 4.3 Data contracts
Each layer publishes a schema contract; downstream depends on the contract, not raw structure.

---

## 5. Pipeline Workflow (Airflow DAG)
```
check_source -> ingest_bronze -> gate1 -> process_silver -> gate2
   -> aggregate_gold -> gate3 -> update_consumption -> alert_notify -> sla_protection
```
On failure: fallback to last-good clone + critical alert (graceful degradation).

---

## 6. Ingestion (10 sources)
IoT (Kafka+Spark) · App Logs (Fluentd->Kafka) · Equipment Master (Airflow+JDBC) · CMMS (API) ·
Weather (Airflow+Python) · SCADA/PLC (OPC-UA+Kafka) · Transaction DB (Debezium CDC) ·
Reference (CSV+Git) · Clickstream (Kafka/Kinesis) · Legacy SFTP (Airflow+SFTP).
*(Tool-per-source rationale in `STATIC_VS_DYNAMIC_INGESTION.md`.)*

---

## 7. Transformation
### 7.1 Silver (10 logics)
Deduplication · Cleansing · Schema enforcement · Standardisation · Filtering · CDC handling ·
Validation · Normalisation · Type casting · Consistency. *(SQL in `SILVER_TRANSFORMATIONS.md`.)*
### 7.2 Gold (materialization goals)
KPI tables · real-time monitoring views · alert/anomaly datasets · SLA metrics · data marts ·
feature store (ML) · reporting tables · API-ready data · data products/contracts.
> Materialisation is where data becomes a product.

---

## 8. Monitoring & Observability
- **Airflow** = workflow alarm (push). **ELK** = log search/RCA. **Prometheus/Grafana** = metrics.
- **1st-line ops:** push alert -> Kibana single-pane -> search by correlation_id -> runbook -> escalate L2/L3.
- **Severity:** CRITICAL -> PagerDuty (30-min escalation) · WARNING -> email digest · OK -> dashboard.

---

## 9. Test & Operations
- **Mock huge data:** Snowflake `GENERATOR` (10M rows) / Databricks `spark.range` / Python.
- **Weekly batch:** dbt Cloud / Databricks Workflows / Snowflake Task / Airflow (`0 2 * * 0`).
- **Tests:** volume · 3-gate quality · idempotency · dedup · data-loss reconciliation · SLA/graceful.
- **Data-loss rule:** `src = bronze` AND `bronze = silver + quarantined + dupes_removed` AND key-completeness = 0.
*(Full plan in `PETROVA_TEST_OPS_MANUAL.md`.)*

---

## 10. Security & Governance
- **RBAC (4 roles):** ADMIN / ENGINEER / ANALYST / READER
- **Controls:** Unity Catalog governance · Key Vault secrets · TLS · row-level security (Power BI) ·
  Time Travel (90d) · Zero-Copy Clone rollback · audit logging
- **Quality gates** enforce trust before each layer.

---

## 11. Tech Stack
Snowflake · Databricks · dbt · Apache Airflow · PySpark · Delta Lake · Kafka · Debezium ·
Azure Data Factory · SSIS · Great Expectations · Power BI · Streamlit · Docker · Terraform ·
ELK · Prometheus · Grafana · PagerDuty.

---

## 12. Appendix - key references
| Topic | Document |
|---|---|
| End-to-end workflow | `END_TO_END_WORKFLOW.md` |
| Ingestion (10 sources) | `STATIC_VS_DYNAMIC_INGESTION.md` |
| Silver transforms | `SILVER_TRANSFORMATIONS.md` |
| Senior end-to-end (UI) | `SENIOR_DE_END_TO_END.md` |
| Non-CLI setup | `PETROVA_NONCLI_SETUP.md` |
| Test & operations | `PETROVA_TEST_OPS_MANUAL.md` |
| Certifications | `CERTIFICATION_TRACK.md` |

---
_PETROVA is a synthetic portfolio platform demonstrating production-grade data engineering patterns. No company-confidential data is included._
