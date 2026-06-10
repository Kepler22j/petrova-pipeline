# 🔄 PETROVA — End-to-End System Architecture & Workflow (master view)
_The single flow that ties every layer + doc together. Updated with the 10-source ingestion + Silver transforms._

## The master workflow (Source → Consumption)
```
 10 SOURCES (static + dynamic)
  IoT · SCADA/OPC-UA · App Logs · SAP/ERP · CDC(Debezium) · CMMS · Weather · Reference · Clickstream · SFTP
        │  (Kafka/Spark · ADF · Snowpipe · Auto Loader · Airflow)
        ▼
 BRONZE  — raw · immutable · append-only · source-aligned
        │  GATE 1: schema / type / not-null
        ▼
 SILVER  — dedupe · cleanse · enforce schema · standardise · validate · SCD2/CDC · enrich (static×dynamic)
        │  GATE 2: business rules
        ▼
 GOLD    — aggregate KPIs · SPC alert engine (6 categories) · immutable marts
        │  GATE 3: statistical / SPC
        ▼
 CONSUMPTION — Power BI (star schema, DAX, RLS) · Streamlit (live) · Alerts (PagerDuty/Email)

 ORCHESTRATION:  Apache Airflow drives every step (idempotent, retries, SLA)
 MONITORING:     ELK (logs) · Prometheus/Grafana (metrics) · Airflow (workflow alerts)
 RESILIENCE:     trigger_rule=all_done + Zero-Copy Clone (dashboards never go dark)
```

## Airflow DAG (the workflow sequence)
```
check_source_availability → ingest_bronze → validate_bronze_gate(G1)
   → process_silver → validate_silver_gate(G2)
   → aggregate_gold → validate_gold_gate(G3)
   → update_consumption → alert_and_notify → sla_protection
```
On failure → fallback to last-good snapshot + critical alert (graceful degradation).

## Stage map → detailed docs
| Stage | What happens | Deep-dive doc |
|---|---|---|
| Sources + Bronze | 10 sources, static vs dynamic, right tool per velocity | `STATIC_VS_DYNAMIC_INGESTION.md` |
| Silver | 10 transformations (dedupe → normalise) with SQL | `SILVER_TRANSFORMATIONS.md` |
| Gold | KPIs + SPC alert engine | README + Notion hub poster |
| Orchestration + BI | Airflow → Power BI/Streamlit | `SENIOR_DE_END_TO_END.md` |
| Setup (UI) | stand it up click-by-click | `PETROVA_NONCLI_SETUP.md` |
| Build plan | 7-day non-CLI labs | `PETROVA_7DAY_NONCLI_LABS.md` |

## 🎤 One-sentence architecture pitch
> *"PETROVA ingests 10 heterogeneous sources — static and dynamic — into an immutable Bronze, refines them through a 10-transform Silver and a 3-gate validation, materializes KPIs + SPC alerts in an immutable Gold, and serves Power BI + Streamlit — all orchestrated by Airflow with SLA-resilient graceful degradation."*
