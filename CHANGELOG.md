# Changelog

All notable changes to the PETROVA project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.3.0] — 2025-05-20

### Added
- `fct_sensor_alerts` — SPC alert engine with 6 categories from 3 primitives (STDDEV, LAG, THRESHOLD)
- Composite severity routing: CRITICAL / WARNING / OK
- Schema tests for all alert categorical columns (`accepted_values`)
- `03_delta_optimization.py` — OPTIMIZE, VACUUM, Z-ORDER notebook (Databricks DE exam)
- Traffic-light priority connections in Architecture SVG (RED/YELLOW/GREEN)
- Python library labels on all architecture connections
- `Makefile` with setup, lint, test, dbt-run, docker-up targets
- `requirements.txt` at project root (11 Python libraries pinned)
- `LICENSE` (MIT)

### Changed
- `talking_points.md` — rewritten with SPC engine, 10 cleaning logics, PTTEP production anchors
- `incident_response.md` — replaced FMEA with SPC alert scenarios, added Delta rollback procedures
- `quality_thresholds.csv` — replaced stale `fmea_rpn` with `stability_stddev`, `signal_cv`, `alert_severity_critical`
- `README.md` — added security posture table, 10 cleaning logics, alert engine section
- `PETROVA_Data_Model.md` — added security & governance model, cleaning logics table
- `.gitignore` — added `*.docx`, `*.xlsx`, `.ipynb_checkpoints/`, `desktop.ini`, `~$*`

### Fixed
- Bronze notebook layer logic: removed cleaning from Bronze (raw only), moved to Silver
- `00_local_delta_lab.ipynb` — each Silver cleaning step labeled with logic number

## [1.2.0] — 2025-05-18

### Added
- `fct_daily_revenue` fact table — daily rollup by material group
- `int_order_line_items` intermediate model — line-level detail with unit price
- `dim_customer` placeholder dimension — awaiting SAP customer source
- Architecture SVG (`PETROVA_Architecture.svg`) — 10-layer visual
- Databricks DLT pipeline (`petrova_dlt_pipeline.py`)
- Unity Catalog setup script
- Databricks workflow JSON definition
- dbt snapshots: `snap_vendor_history` (timestamp strategy), `snap_equipment_history` (check strategy)
- dbt seeds: `quality_thresholds.csv`, `sensor_status_codes.csv`
- dbt exposures and metrics YAML
- dbt documentation blocks (`petrova_docs.md`)

### Changed
- `dbt_project.yml` — added vars for critical thresholds
- Intermediate models materialized as incremental

## [1.1.0] — 2025-05-16

### Added
- Snowflake DDL: warehouses, schemas, Bronze/Silver/Gold tables, stages, tasks
- Snowflake semi-structured processing (FLATTEN, LATERAL, PARSE_JSON)
- Snowflake Streams for change tracking
- Snowflake Time Travel (AT/BEFORE, UNDROP)
- Snowflake data sharing + security (secure views, dynamic masking)
- RBAC hierarchy: 4 roles (ADMIN → ENGINEER → ANALYST → READER)
- Gold immutability check stored procedure
- MERGE procedure for sensor KPI
- UDF examples for Snowflake
- Terraform infrastructure (main.tf, variables.tf, outputs.tf)
- CI pipeline (`.github/workflows/ci.yml`) — SQLFluff + Ruff + dbt test
- Great Expectations suite + checkpoint
- PagerDuty monitoring config
- Pipeline notification stored procedure

## [1.0.0] — 2025-05-14

### Added
- Initial Medallion Architecture: Bronze → Silver → Gold
- 5 staging models (views): sensor_readings, sap_orders, sap_vendors, sap_materials, equipment_master
- 4 intermediate models: sensor_cleaned (3-Gate), equipment_enriched, orders_validated, scd2_vendors
- 2 Gold facts: `fct_daily_sensor_kpi`, `fct_daily_revenue`
- 2 Gold dimensions: `dim_equipment`, `dim_vendor`
- 4 dbt macros: `three_gate_validation`, `scd2_merge`, `audit_columns`, `generate_schema_name`
- Airflow DAG with Docker environment
- Databricks notebooks: local Delta lab, Bronze ingest, Structured Streaming
- `README.md`, `PETROVA_Data_Model.md`
