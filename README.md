<div align="center">

# PETROVA

### Production-Grade Data Platform with Spark, Databricks, dbt, Snowflake, Airflow & Streamlit Real-Time Dashboard (400K+ Records/Day)

[![CI](https://github.com/Kepler22j/petrova-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Kepler22j/petrova-pipeline/actions)
[![dbt](https://img.shields.io/badge/dbt-1.8.7-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Terraform](https://img.shields.io/badge/Terraform-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io/)

**Medallion Architecture · PySpark · Delta Lake · dbt · Snowflake · Airflow · Terraform · SPC Monitoring**
**10 Architecture Layers · 16 dbt Models · 3-Gate Validation · Real-Time Alert Engine · Enterprise Security**

[Architecture](#architecture) · [Quick Start](#quick-start) · [Medallion Layers](#medallion-architecture) · [Key Features](#key-features) · [Tech Stack](#tech-stack)

</div>

---

## Overview

PETROVA is a **production-grade hybrid cloud data platform** that bridges on-premises legacy systems (SAP, SSIS, SQL Server) with modern cloud services (Snowflake, Databricks, Azure Data Factory). It implements a **Medallion Architecture** (Bronze → Silver → Gold) with 10 structured cleaning logics, SCD Type 2 history tracking, and a **Statistical Process Control (SPC) alert engine** for real-time equipment monitoring.

Built with a **security-first mindset** (4-role RBAC, AES-256, TLS 1.2+, Azure Key Vault, Unity Catalog) and **SRE operational discipline** (1,200+ alert rules, composite severity routing, 99.5% SLA). Inspired by real-world APM platform experience on offshore oil rigs processing 400,000+ sensor records/day.

## Security Posture

| Layer | Control | Standard |
|-------|---------|----------|
| **Network** | TLS 1.2+ everywhere, VPN for on-prem, DMZ ingress | ISO 27001 aligned |
| **Authentication** | LDAP/SSO, Azure AD, Snowflake key-pair auth | Zero-trust |
| **Encryption** | AES-256 at rest, TLS in transit, Key Vault for secrets | FIPS 140-2 |
| **Authorization** | 4-role RBAC hierarchy, Unity Catalog fine-grained ACL | Least privilege |
| **Data Protection** | Dynamic data masking, Gold immutability (7 Commandments) | PCI-DSS aligned |
| **Audit** | Pipeline run logging, DESCRIBE HISTORY, Time Travel 90d | SOX-ready |
| **DevSecOps** | CI security scanning (SQLFluff, SonarQube), PR review gates | Shift-left |
| **Disaster Recovery** | ADLS lifecycle (hot→cool→archive), Fail-Safe 7d, Zero-Copy Clone | RPO < 1hr |

## Architecture

<div align="center">

```
┌─────────────────────────────────────────────────────────────────┐
│                     PETROVA ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│  │  SOURCE   │   │  INGEST  │   │  BRONZE  │   │  SILVER  │    │
│  │  SYSTEMS  │──▶│  LAYER   │──▶│  (Raw)   │──▶│ (Clean)  │    │
│  │ SAP/IoT   │   │ ADF/SSIS │   │ Schema ✓ │   │ Quality ✓│    │
│  └──────────┘   └──────────┘   └──────────┘   └────┬─────┘    │
│                                                      │          │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌────▼─────┐    │
│  │ MONITOR  │   │ PRESENT  │   │ SEMANTIC  │   │   GOLD   │    │
│  │ PagerDuty│◀──│ Power BI │◀──│   dbt    │◀──│ (Business)│    │
│  │ Alerts   │   │ Streamlit│   │  Metrics  │   │  SPC  ✓  │    │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘    │
│                                                                  │
│  ORCHESTRATION: Airflow 2.9.3 + ADF (Azure) + SSIS (On-Prem)   │
│  COMPUTE: PySpark 3.5.3 + Delta Lake 3.2.1                     │
│  INFRASTRUCTURE: Terraform · GitHub Actions CI/CD               │
└─────────────────────────────────────────────────────────────────┘
```

</div>

> Full interactive SVG diagram: [`docs/architecture/PETROVA_Architecture.svg`](docs/architecture/PETROVA_Architecture.svg)

## Documentation

Full docs in [`docs/`](docs/). **Start here:** [`PETROVA_SYSTEM_MANUAL.md`](docs/PETROVA_SYSTEM_MANUAL.md) — architecture · data architecture · application design · operations.

| Doc | Purpose |
|-----|---------|
| [System Manual](docs/PETROVA_SYSTEM_MANUAL.md) ⭐ | master: architecture + data architecture + app design + ops |
| [End-to-End Workflow](docs/END_TO_END_WORKFLOW.md) | source → consumption flow + Airflow DAG |
| [Ingestion: Static vs Dynamic](docs/STATIC_VS_DYNAMIC_INGESTION.md) | 10 ingestion sources, tool per velocity |
| [Silver Transformations](docs/SILVER_TRANSFORMATIONS.md) | 10 Silver transforms (SQL) |
| [Senior DE End-to-End](docs/SENIOR_DE_END_TO_END.md) | full-stack non-CLI + senior decisions |
| [Non-CLI Setup](docs/PETROVA_NONCLI_SETUP.md) | UI whole-pipeline setup |
| [Test & Ops Manual](docs/PETROVA_TEST_OPS_MANUAL.md) | mock data · weekly batch · data-loss test |
| [Certification Track](docs/CERTIFICATION_TRACK.md) | SnowPro / Databricks / dbt mapped to the project |
| [Project Status](docs/PROJECT_STATUS.md) | current state + data-model roadmap |

## Quick Start

```bash
# 1. Clone & install
git clone https://github.com/Kepler22j/petrova-pipeline.git
cd petrova-pipeline
make setup                        # Install all Python deps + dbt packages

# 2. Environment setup
cp .env.example .env              # Edit with your Snowflake/Azure/Databricks credentials

# 3. Validate & lint
make validate                     # Check environment readiness
make lint                         # SQLFluff + Ruff

# 4. Run Snowflake DDL (in order)
# Execute: snowflake/ddl/01_warehouse.sql through 06_stages_and_tasks.sql
# Execute: snowflake/rbac/roles_and_grants.sql

# 5. Run dbt pipeline
make dbt-seed                     # Load reference data
make dbt-run                      # Bronze → Silver → Gold
make dbt-test                     # Run all quality tests

# 6. Start Airflow + PySpark + Jupyter (local dev)
make docker-up                    # Start all Docker services

# 7. Access services
# Airflow UI:    http://localhost:8080 (admin/admin)
# Jupyter Lab:   http://localhost:8888 (token: petrova)
# Spark UI:      http://localhost:4040 (when job runs)

# 8. Run PySpark Delta Lab
# Open Jupyter → notebooks/00_local_delta_lab.ipynb
# Run all cells: Bronze → Silver → Gold with Delta Lake
```

## Medallion Architecture

### Bronze Layer (Raw / Immutable)
- **Purpose**: Ingest raw data exactly as received — no transformations
- **Sources**: SAP BODS (orders, materials, vendors), IoT sensors (Parquet), equipment master
- **Validation**: Bronze Gate — schema completeness checks
- **Models**: `stg_sensor_readings`, `stg_sap_orders`, `stg_sap_materials`, `stg_sap_vendors`, `stg_equipment_master`
- **Location**: `dbt/models/staging/`

### Silver Layer (Cleaned / SCD2) — 10 Cleaning Logics
- **Purpose**: Clean, validate, enrich, and historize data using **10 structured cleaning logics**
- **Key Pattern**: SCD Type 2 via SSIS Lookup + Snowflake MERGE
- **Validation**: Silver Gate — statistical quality rules (dbt_expectations + Great Expectations)
- **Models**: `int_sensor_cleaned`, `int_orders_validated`, `scd2_vendors`, `int_equipment_enriched`, `int_order_line_items`
- **Location**: `dbt/models/intermediate/`

**10 Silver Cleaning Logics (3-Gate Framework):**

| Gate | Logic | Name | Implementation |
|------|-------|------|----------------|
| Gate 1: Schema | 1 | Null Detection & Flagging | `isNull()` → quality_flag = FAIL |
| Gate 1: Schema | 2 | Data Type Casting | `cast()`, StructType enforcement |
| Gate 1: Schema | 3 | Range Validation | `BETWEEN` checks, physical limits |
| Gate 2: Business | 4 | Status Filtering | Remove INACTIVE equipment |
| Gate 2: Business | 5 | Deduplication | `ROW_NUMBER() OVER(PARTITION BY...)` |
| Gate 2: Business | 6 | Derived Columns | `reading_date`, `reading_hour`, lifecycle_stage |
| Gate 2: Business | 7 | SCD Type 2 Merge | `row_hash`, `is_current`, `valid_from/to` |
| Gate 3: Cross-Table | 8 | Referential Integrity | JOIN checks against master data |
| Gate 3: Cross-Table | 9 | Aggregation Guards | HAVING / COUNT checks |
| Gate 3: Cross-Table | 10 | Late-Arriving Data | Watermark-based windowing |

### Gold Layer (Business-Ready / Immutable)
- **Purpose**: Aggregated KPIs and dimensions for BI consumption
- **Protection**: 7 Gold Immutability Commandments + 4-role RBAC
- **Validation**: Gold Gate — SPC alert engine (composite severity: CRITICAL/WARNING/OK)
- **Models**: `fct_daily_sensor_kpi`, `fct_daily_revenue`, `fct_sensor_alerts`, `dim_equipment`, `dim_vendor`, `dim_customer`
- **Alert Engine**: `fct_sensor_alerts` — 6 alert categories from 3 statistical primitives (STDDEV, LAG, THRESHOLD) with composite severity scoring (CRITICAL/WARNING/OK)
- **Location**: `dbt/models/marts/`

## Key Features

### 3-Gate Validation
Every record passes through three independent quality gates before reaching Gold:

| Gate | Layer | Method | Blocks On |
|------|-------|--------|-----------|
| Bronze Gate | Raw → Bronze | Schema validation, NOT NULL checks | Missing required columns |
| Silver Gate | Bronze → Silver | Statistical rules, dbt_expectations | Quality score < threshold |
| Gold Gate | Silver → Gold | SPC alert engine (STDDEV + LAG + THRESHOLD) | Composite severity = CRITICAL |

### Triple Orchestration
A unique pattern combining three orchestration engines:

| Engine | Role | Scope |
|--------|------|-------|
| **Apache Airflow** | Primary orchestrator | Cloud pipeline scheduling, DAG management |
| **Azure Data Factory** | Cloud ingestion | Blob → Snowflake Parquet loads |
| **SSIS** | Legacy bridge | On-prem SAP extraction, SCD2 Lookup |

### Gold Immutability — 7 Commandments
1. No direct INSERT/UPDATE/DELETE on Gold tables
2. All writes via stored procedures only
3. MERGE pattern for all upserts
4. Zero-Copy Clone for instant rollback (< 1 second)
5. Audit every mutation to `AUDIT.PIPELINE_LOG`
6. RBAC enforcement (4-role hierarchy)
7. Automated validation post-write

### SCD Type 2 Implementation
Full history tracking for slowly changing dimensions:
- **Detection**: SSIS Lookup Transform compares row hashes
- **Execution**: Snowflake MERGE expires old rows, inserts new versions
- **Columns**: `is_current`, `valid_from`, `valid_to`, `row_hash`
- **dbt Macro**: `scd2_merge()` for reusable SCD2 logic

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Cloud DW** | Snowflake (Medallion schemas, Snowpipe, Zero-Copy Clone) |
| **Lakehouse** | Databricks (Delta Lake, Unity Catalog) |
| **Transforms** | dbt-core (models, macros, tests, packages) |
| **Orchestration** | Apache Airflow, Azure Data Factory, SSIS |
| **Data Quality** | Great Expectations, dbt_expectations, custom 3-Gate macros |
| **Source Systems** | SAP BODS, IoT sensors (Parquet), equipment systems |
| **Visualization** | Power BI, Streamlit, SSRS |
| **IaC** | Terraform (Azure + Snowflake providers) |
| **CI/CD** | GitHub Actions (SQLFluff, dbt compile, Terraform validate) |
| **Monitoring** | PagerDuty, Azure Monitor, Snowflake email integration |
| **Security** | RBAC (4-role hierarchy), Azure Key Vault, AES-256, TLS 1.2+, Unity Catalog |
| **Python Libraries** | pyspark, delta-spark, dbt-core, great-expectations, pandas, pyarrow, snowflake-connector-python, pyodbc, sqlalchemy, requests, jinja2 |

## Project Structure

```
petrova-pipeline/
├── .github/workflows/      # CI/CD (lint, dbt test, terraform validate)
├── airflow/                # Dockerized orchestration stack
│   ├── dags/               #   petrova_validated_pipeline.py
│   ├── docker-compose.yml  #   Airflow + Jupyter-Spark + PostgreSQL
│   └── Dockerfile          #   Custom image (PySpark 3.5.3 + dbt + Delta)
├── assets/                 # Presentation deck (PDF + slide PNGs)
├── databricks/             # Databricks notebooks & pipelines
│   ├── notebooks/          #   00_local_delta_lab, 01_bronze_ingest, 02_streaming, 03_delta_optimization
│   ├── dlt/                #   Delta Live Tables pipeline
│   ├── unity_catalog/      #   Unity Catalog governance setup
│   └── workflows/          #   Databricks Workflow JSON configs
├── dbt/                    # dbt-core project
│   ├── macros/             #   generate_schema_name, audit, scd2_merge, 3-gate
│   ├── models/
│   │   ├── staging/        #   Bronze (5 models + sources + schema tests)
│   │   ├── intermediate/   #   Silver (4 models + SCD2 + schema tests)
│   │   └── marts/          #   Gold (6 models: 3 facts + 3 dims + schema tests)
│   └── packages.yml        #   dbt_utils, dbt_expectations, codegen
├── docs/                   # START: PETROVA_SYSTEM_MANUAL.md + PROJECT_STATUS.md
│   ├── architecture/       #   SVG architecture diagram
│   ├── workflow_diagrams/  #   8 SVG pattern diagrams
│   ├── runbooks/           #   incident_response, k8s_incident_playbook
│   ├── upskill/            #   Databricks / dbt / Snowflake lab guides
│   ├── PETROVA_SYSTEM_MANUAL.md      #   master: architecture + data arch + app design + ops
│   ├── END_TO_END_WORKFLOW.md        #   master flow + Airflow DAG
│   ├── STATIC_VS_DYNAMIC_INGESTION.md · SILVER_TRANSFORMATIONS.md
│   ├── SENIOR_DE_END_TO_END.md · PETROVA_NONCLI_SETUP.md · PETROVA_TEST_OPS_MANUAL.md
│   └── CERTIFICATION_TRACK.md · PETROVA_Data_Model.md · DASHBOARD_GUIDE.md · PROJECT_STATUS.md
├── great_expectations/     # Data quality suites & checkpoints
├── monitoring/             # PagerDuty config, notification procedures
├── scripts/                # Test generators, backup, env validation
│   ├── simulate_hourly_drops.py  # IoT sensor batch simulator (400K records)
│   ├── chaos_test.py             # 8 chaos tests for 3-Gate resilience
│   └── generate_sap_load.py      # PySpark 10-20 GB load generator
├── snowflake/
│   ├── ddl/                #   Warehouses, schemas, all table DDL (Bronze/Silver/Gold)
│   ├── procedures/         #   MERGE procedures, Gold immutability checks
│   └── rbac/               #   4-role hierarchy, grants, future grants
├── ssis/                   # SSIS package documentation + C# reference
├── terraform/              # Azure + Snowflake IaC (main, variables, outputs)
├── tests/
│   ├── integration/        #   dbt compile/test validation
│   └── e2e/                #   Full Bronze→Silver→Gold flow tests
├── streamlit_app.py        # Real-time ops dashboard (Streamlit, repo root)
├── .env.example            # Credential template
├── .gitignore              # Python, dbt, Airflow, Terraform, secrets
├── .sqlfluff               # SQL linting rules (Snowflake dialect)
└── README.md               # ← You are here
```

## RBAC Model

```
PETROVA_ADMIN          ← Full access, DDL, procedure management
    └── PETROVA_ENGINEER   ← ETL writes (Bronze/Silver), procedure execution
        └── PETROVA_ANALYST    ← Read Bronze/Silver, query Gold
            └── PETROVA_READER     ← Read-only Gold layer (BI tools)
```

## Monitoring & Alerting

| Alert | Severity | Channel |
|-------|----------|---------|
| Bronze Gate failure | Warning | PagerDuty + Email |
| Silver Gate failure | Error | PagerDuty + Email |
| Gold Gate CRITICAL alert | Critical | PagerDuty → On-call → Manager |
| Pipeline timeout | Error | PagerDuty |
| Snowpipe lag > 30min | Warning | PagerDuty |

### Statistical Alert Engine (`fct_sensor_alerts`)
6 alert categories derived from 3 statistical primitives (SPC — Statistical Process Control):

| Alert | Primitive | Logic | Threshold |
|-------|-----------|-------|-----------|
| Stability Level | STDDEV | stddev bands | < 5 STABLE, 5-25 NORMAL, > 25 UNSTABLE |
| Signal Quality | STDDEV/AVG | Coefficient of Variation | CV > 0.3 = NOISY, > 0.5 = VERY_NOISY |
| Outlier Detection | STDDEV | 2σ rule | max > avg + 2×stddev |
| Range Violation | STDDEV | 1σ expected band | outside avg ± 1×stddev |
| Volatility Trend | LAG(STDDEV) | Day-over-day stddev change | > 1.5× previous = DEGRADING |
| Spike Detection | LAG(MAX) | Sudden jump detection | delta > 2×stddev = SPIKE |

**Composite Severity**: CRITICAL (multiple triggers) → WARNING (single trigger) → OK (clean)
**Routing**: CRITICAL → PagerDuty 30min escalation · WARNING → Email · OK → Dashboard only

## Development

```bash
# Lint SQL
sqlfluff lint dbt/models/ --dialect snowflake

# Lint Python
ruff check airflow/ dashboards/ scripts/

# Run dbt tests
cd dbt && dbt test --target dev --profiles-dir .

# Run integration tests
pytest tests/integration/ -v

# Run e2e tests (requires Snowflake connection)
pytest tests/e2e/ -v

# Terraform plan
cd terraform && terraform plan -var="environment=dev"
```

## Local Dev Stack (Docker)

The project includes a complete local development environment via Docker Compose:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| PostgreSQL 15 | `postgres:15-alpine` | 5432 | Airflow metadata |
| Airflow Webserver | Custom (apache/airflow:2.9.3) | 8080 | DAG management UI |
| Airflow Scheduler | Custom (apache/airflow:2.9.3) | - | Task execution |
| Jupyter + PySpark | `quay.io/jupyter/pyspark-notebook:spark-3.5.3` | 8888 / 4040 | Delta Lake notebook dev |

Jupyter serves as a **mini CI/CD gate**: develop and test PySpark + Delta Lake locally, then push to Git for Databricks production deployment.

## Certification Alignment

Every component maps to at least one exam domain:

| Component | Databricks DE Associate | SnowPro Core (COF-C03) | dbt Analytics Engineer |
|-----------|------------------------|----------------------|----------------------|
| Auto Loader / COPY INTO | Ingestion patterns | Data loading | - |
| Delta MERGE / SCD2 | Delta operations | - | Incremental models |
| Structured Streaming | Streaming concepts | Snowpipe | - |
| Window Functions | DataFrame API | SQL windows | SQL patterns |
| Medallion Architecture | Best practices | Data architecture | Model layers |
| Unity Catalog | Governance | RBAC / Secure Views | - |
| dbt Models & Tests | - | - | Full coverage |

## Workflow Pattern Diagrams

8 production workflow diagrams in `docs/workflow_diagrams/` — each with flow paths, decision logic, key concepts, and interview answers:

| # | Pattern | Key Concept |
|---|---------|-------------|
| 1 | SLA Protection | `trigger_rule=all_done` + fallback snapshot |
| 2 | 3-Gate Validation | Schema → Business → Referential with quarantine |
| 3 | SPC Alert Engine | 3 primitives → 6 alerts → composite severity |
| 4 | SCD Type 2 | Timestamp + Check strategies, cross-DB macro |
| 5 | Quarantine & Recovery | Tagged records, zero data loss guarantee |
| 6 | Cross-DB Compatibility | Write once, run on PostgreSQL + Snowflake |
| 7 | Incremental Processing | Auto Loader + `is_incremental()` |
| 8 | Local E2E Pipeline | Docker Compose → 6 services → 51/51 tests |

## Testing (4-Level Pyramid)

| Level | Scope | Tool | Data Size |
|-------|-------|------|-----------|
| **L1 — Unit** | dbt seeds + model tests | `dbt test` (51 tests) | ~1K rows |
| **L2 — Integration** | Docker Compose full pipeline | `make docker-up` → E2E | ~10K rows |
| **L3 — Load** | PySpark on Databricks | `generate_sap_load.py` | 10-20 GB (50M rows) |
| **L4 — Chaos** | Intentionally broken data | `chaos_test.py` | 8 failure scenarios |

### Test Scripts (`scripts/`)

**`simulate_hourly_drops.py`** — Generates synthetic IoT sensor batches (400K records) with realistic distributions: 50 equipment units, 4 sensor types, 95% PASS / 3% WARN / 2% FAIL. Supports `--continuous` mode for hourly drops.

**`chaos_test.py`** — 8 chaos tests targeting each gate: missing columns, wrong types, all nulls, duplicates, late-arriving data, empty files, extreme outliers (100x normal), negative values. Expected catch mapping: Gate 1 (tests 1,2,3,6), Gate 2 (tests 4,5,8), Gate 3 (test 7).

**`generate_sap_load.py`** — Dual-mode generator: PySpark (Databricks, 50M+ rows for 10-20 GB load tests) and pandas (local, up to 5M rows). Generates SAP orders, materials master, and IoT sensor readings at scale.

```bash
# Generate sensor batch (local)
python scripts/simulate_hourly_drops.py --records 100000

# Run chaos tests
python scripts/chaos_test.py --test 1 3 5

# Generate SAP load (local/pandas)
python scripts/generate_sap_load.py --rows 1000000 --output data/landing/sap/

# Generate SAP load (Databricks/PySpark — 10-15 GB)
# df = generate_sap_orders_spark(spark, 50_000_000)
# df.write.parquet("/mnt/landing/sap_orders_full/")
```

## Controlled Trade-offs

Every design decision has a documented mitigation:

| Trade-off | Risk Level | Controlled? | Mitigation |
|-----------|-----------|-------------|------------|
| Complexity | High | Yes | One-command setup (`make run`) |
| Alert Fatigue | Medium | Yes | Composite severity (3 levels) |
| Logic Duplication | Medium | Yes | dbt = single source of truth |
| Ghost Success | High | Yes | Freshness flag + PagerDuty |
| Cost | Medium | Yes | Auto-suspend + daily batch |
| Latency | Low | Intentional | 400K/day doesn't justify streaming |
| Cascade Failure | Low | Expected | Fail-fast over silent corruption |
| Skill Requirement | Medium | Org-dependent | Hiring criteria, not system flaw |

## License

MIT License. See [LICENSE](LICENSE) for details.

---

<div align="center">
<strong>PETROVA — Production-Grade Data Platform</strong><br>
<em>Spark · Databricks · dbt · Snowflake · Delta Lake · Airflow · Terraform · SPC Monitoring</em><br>
<em>By Jay Pechnarai — Senior Data Platform Engineer · 20+ Years Mission-Critical Systems</em>
</div>
