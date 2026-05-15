<div align="center">

# PETROVA 300K

### Hybrid Cloud Data Platform — Production-Grade Portfolio

[![CI](https://github.com/YOUR_USERNAME/petrova-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/petrova-pipeline/actions)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B?logo=dbt)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Terraform](https://img.shields.io/badge/Terraform-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io/)

**9 Architecture Layers · 24 Pipeline Steps · 25+ Technologies · 3-Gate Validation**

[Architecture](#architecture) · [Quick Start](#quick-start) · [Medallion Layers](#medallion-architecture) · [Key Features](#key-features) · [Tech Stack](#tech-stack)

</div>

---

## Overview

PETROVA 300K is a **hybrid cloud data platform** that bridges on-premises legacy systems (SAP, SSIS, SQL Server) with modern cloud services (Snowflake, Databricks, Azure Data Factory). It implements a **Medallion Architecture** with enterprise-grade data quality gates, SCD Type 2 history tracking, and FMEA-based risk validation.

This project demonstrates end-to-end data engineering at the scale and rigor expected in **$300K+ senior/staff data engineering roles**.

## Architecture

<div align="center">

```
┌─────────────────────────────────────────────────────────────────┐
│                    PETROVA 300K ARCHITECTURE                     │
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
│  │ Alerts   │   │ Streamlit│   │  Metrics  │   │  FMEA ✓  │    │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘    │
│                                                                  │
│  ORCHESTRATION: Airflow (Cloud) + ADF (Azure) + SSIS (On-Prem) │
│  INFRASTRUCTURE: Terraform · GitHub Actions CI/CD               │
└─────────────────────────────────────────────────────────────────┘
```

</div>

> Full interactive SVG diagram: [`docs/architecture/PETROVA_Architecture.svg`](docs/architecture/PETROVA_Architecture.svg)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/petrova-pipeline.git
cd petrova-pipeline

# 2. Environment setup
cp .env.example .env
# Edit .env with your Snowflake/Azure/Databricks credentials

# 3. Validate environment
bash scripts/validate_environment.sh

# 4. Install dbt dependencies
cd dbt && dbt deps --profiles-dir . && cd ..

# 5. Run Snowflake DDL (in order)
# Execute: snowflake/ddl/01_warehouse.sql through 06_stages_and_tasks.sql
# Execute: snowflake/rbac/roles_and_grants.sql

# 6. Run dbt pipeline
cd dbt
dbt run --select tag:bronze --target dev --profiles-dir .
dbt run --select tag:silver --target dev --profiles-dir .
dbt run --select tag:gold   --target dev --profiles-dir .
dbt test --target dev --profiles-dir .

# 7. Start Airflow (local dev)
cd ../airflow && docker-compose up -d

# 8. Launch Streamlit dashboard
cd ../dashboards/streamlit && streamlit run app.py
```

## Medallion Architecture

### Bronze Layer (Raw / Immutable)
- **Purpose**: Ingest raw data exactly as received — no transformations
- **Sources**: SAP BODS (orders, materials, vendors), IoT sensors (Parquet), equipment master
- **Validation**: Bronze Gate — schema completeness checks
- **Models**: `stg_sensor_readings`, `stg_sap_orders`, `stg_sap_materials`, `stg_sap_vendors`, `stg_equipment_master`
- **Location**: `dbt/models/staging/`

### Silver Layer (Cleaned / SCD2)
- **Purpose**: Clean, validate, enrich, and historize data
- **Key Pattern**: SCD Type 2 via SSIS Lookup + Snowflake MERGE
- **Validation**: Silver Gate — statistical quality rules (dbt_expectations + Great Expectations)
- **Models**: `int_sensor_cleaned`, `int_orders_validated`, `scd2_vendors`, `int_equipment_enriched`
- **Location**: `dbt/models/intermediate/`

### Gold Layer (Business-Ready / Immutable)
- **Purpose**: Aggregated KPIs and dimensions for BI consumption
- **Protection**: 7 Gold Immutability Commandments + 4-role RBAC
- **Validation**: Gold Gate — FMEA risk scoring (blocks if RPN > threshold)
- **Models**: `fct_daily_sensor_kpi`, `fct_daily_revenue`, `dim_equipment`, `dim_vendor`, `dim_customer`
- **Location**: `dbt/models/marts/`

## Key Features

### 3-Gate Validation
Every record passes through three independent quality gates before reaching Gold:

| Gate | Layer | Method | Blocks On |
|------|-------|--------|-----------|
| Bronze Gate | Raw → Bronze | Schema validation, NOT NULL checks | Missing required columns |
| Silver Gate | Bronze → Silver | Statistical rules, dbt_expectations | Quality score < threshold |
| Gold Gate | Silver → Gold | FMEA risk assessment | Risk Priority Number > 100 |

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
| **Security** | RBAC (4-role hierarchy), Azure Key Vault, Gold Immutability |

## Project Structure

```
petrova-pipeline/
├── .github/workflows/      # CI/CD (lint, dbt test, terraform validate)
├── adf/                    # Azure Data Factory pipeline & linked service JSON
├── airflow/                # Airflow DAG, docker-compose, config
│   └── dags/               #   petrova_validated_pipeline.py
├── dashboards/             # Visualization layer
│   ├── streamlit/          #   Real-time ops dashboard (app.py)
│   ├── powerbi/            #   Power BI connection guide
│   └── ssrs/               #   SSRS scheduled reports
├── dbt/                    # dbt-core project
│   ├── macros/             #   generate_schema_name, audit, scd2_merge, 3-gate
│   ├── models/
│   │   ├── staging/        #   Bronze (5 models + sources + schema tests)
│   │   ├── intermediate/   #   Silver (4 models + SCD2 + schema tests)
│   │   └── marts/          #   Gold (5 models + schema tests)
│   └── packages.yml        #   dbt_utils, dbt_expectations, codegen
├── docs/
│   ├── architecture/       #   SVG architecture diagram
│   ├── interview_prep/     #   Talking points for $300K+ interviews
│   └── runbooks/           #   Incident response procedures
├── great_expectations/     # Data quality suites & checkpoints
├── monitoring/             # PagerDuty config, notification procedures
├── scripts/                # Backup (Zero-Copy Clone), env validation
├── snowflake/
│   ├── ddl/                #   Warehouses, schemas, all table DDL (Bronze/Silver/Gold)
│   ├── procedures/         #   MERGE procedures, Gold immutability checks
│   └── rbac/               #   4-role hierarchy, grants, future grants
├── ssis/                   # SSIS package documentation + C# reference
├── terraform/              # Azure + Snowflake IaC (main, variables, outputs)
├── tests/
│   ├── integration/        #   dbt compile/test validation
│   └── e2e/                #   Full Bronze→Silver→Gold flow tests
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
| Gold Gate FMEA block | Critical | PagerDuty → On-call → Manager |
| Pipeline timeout | Error | PagerDuty |
| Snowpipe lag > 30min | Warning | PagerDuty |

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

## License

This project is a portfolio demonstration. All code is original work by **Jay Pechnarai (Jakapong Pechnarai)**.

---

<div align="center">
<strong>Built for $300K+ Data Engineering roles</strong><br>
<em>9 Layers · 24 Steps · 25+ Technologies · Zero Compromise</em>
</div>
