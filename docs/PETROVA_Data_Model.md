# PETROVA 300K — Data Model Documentation

## Data Lineage Overview

```
SOURCE SYSTEMS                    BRONZE (Raw)                  SILVER (Cleaned)              GOLD (Business)
──────────────                    ────────────                  ────────────────              ──────────────

IoT Sensors (Parquet) ──────►  stg_sensor_readings ──────►  int_sensor_cleaned ──────►  fct_daily_sensor_kpi
                                   │ sensor_id                    │ quality_flag (3-Gate)       │ kpi_sk
                                   │ reading_value                │ is_valid                    │ sensor_id
                                   │ reading_timestamp            │ reading_date                │ reading_date
                                   │ sensor_name                  │ reading_hour                │ avg_reading
                                   │ equipment_name               │ dedup (ROW_NUMBER)          │ max/min/stddev
                                                                                                │ total_readings

SAP Orders (BODS) ─────────►  stg_sap_orders ──────────►  int_orders_validated ─────►  fct_daily_revenue
                                   │ order_number                 │ quality_flag                │ revenue_sk
                                   │ order_date                   │ business rules applied      │ revenue_date
                                   │ material_group               │                             │ material_group
                                   │ net_value                    │                             │ order_count
                                   │ order_quantity               │                             │ total_revenue

SAP Vendors (BODS) ────────►  stg_sap_vendors ─────────►  scd2_vendors ─────────────►  dim_vendor
                                   │ vendor_number                │ vendor_sk                   │ vendor_sk
                                   │ vendor_name                  │ is_current                  │ vendor_number
                                   │ country                      │ valid_from / valid_to       │ is_current
                                   │ payment_terms                │ row_hash                    │ valid_from/to

SAP Materials (BODS) ──────►  stg_sap_materials ────────►  int_order_line_items ──────►  (joined in facts)
                                   │ material_id                  │ line-level detail
                                   │ material_group               │
                                   │ unit_of_measure              │

Equipment Registry ────────►  stg_equipment_master ─────►  int_equipment_enriched ────►  dim_equipment
                                   │ equipment_id                 │ equipment_sk                │ equipment_sk
                                   │ equipment_name               │ days_since_install          │ equipment_id
                                   │ equipment_type               │ lifecycle_stage             │ lifecycle_stage
                                   │ location                     │ multi-source JOIN           │ _gold_loaded_at
                                   │ install_date                 │

                                                                  fct_daily_sensor_kpi ────►  fct_sensor_alerts
                                                                                                    │ alert_sk
                                                                                                    │ stability_level
                                                                                                    │ signal_quality
                                                                                                    │ spike_status
                                                                                                    │ alert_severity
                                                                                                    │ (SPC: STDDEV+LAG+THRESHOLD)

                                                                                                dim_customer
                                                                                                │ customer_sk
                                                                                                │ (placeholder)
```

---

## Layer Details

### Staging (Bronze) — 5 Models

All staging models are materialized as **views** for zero-storage overhead.

| Model | Source Table | Key Columns | Transformation |
|-------|------------|-------------|----------------|
| `stg_sensor_readings` | `raw.sensor_readings_parquet` | sensor_id, reading_value, reading_timestamp, sensor_name, equipment_name, status, process_area | Column renaming, type casting |
| `stg_sap_orders` | `raw.raw_sap_orders` | order_number, order_date, material_group, net_value, order_quantity, status | Date parsing, status normalization |
| `stg_sap_vendors` | `raw.raw_sap_vendors` | vendor_number, vendor_name, country, payment_terms | Address standardization |
| `stg_sap_materials` | `raw.raw_sap_materials` | material_id, material_group, unit_of_measure | Unit conversion |
| `stg_equipment_master` | `raw.raw_equipment_master` | equipment_id, equipment_name, equipment_type, location, install_date, status | Surrogate key generation |

### Intermediate (Silver) — 5 Models

Silver models apply 3-Gate validation and are materialized as **tables**.

| Model | Materialization | Key Feature | Quality Gate |
|-------|----------------|-------------|--------------|
| `int_sensor_cleaned` | table | 3-Gate validation, null flagging, range check, dedup | Gate 1 + Gate 2 |
| `int_equipment_enriched` | table | Multi-source JOIN enrichment, lifecycle staging | Gate 2 |
| `int_orders_validated` | table | Business rule validation, quality flagging | Gate 2 |
| `int_order_line_items` | table | Line-level detail extraction from orders | Gate 2 |
| `scd2_vendors` | incremental | SCD Type 2 slowly changing dimension (is_current, valid_from/to, row_hash) | Gate 2 + Gate 3 |

### Marts (Gold) — 6 Models (3 Facts + 3 Dimensions)

Gold models are protected by the 7 Immutability Commandments and RBAC.

| Model | Type | Key Columns | Business Logic |
|-------|------|-------------|----------------|
| `fct_daily_sensor_kpi` | Fact | kpi_sk, sensor_id, reading_date, avg_reading, max_reading, min_reading, stddev_reading, total_readings | Only PASS/WARN readings, daily aggregation |
| `fct_daily_revenue` | Fact | revenue_sk, revenue_date, material_group, order_count, total_quantity, total_revenue | Only validated orders, daily rollup by material |
| `fct_sensor_alerts` | Fact | alert_sk, sensor_id, kpi_date, stability_level, signal_quality, has_outlier, range_violated, volatility_trend, spike_status, alert_severity | SPC engine: 6 alerts from 3 primitives (STDDEV, LAG, THRESHOLD), composite severity (CRITICAL/WARNING/OK) |
| `dim_equipment` | Dimension | equipment_sk, equipment_id, equipment_name, equipment_type, location, lifecycle_stage | From int_equipment_enriched |
| `dim_vendor` | Dimension (SCD2) | vendor_sk, vendor_number, vendor_name, country, is_current, valid_from, valid_to | Current + historical view |
| `dim_customer` | Dimension | customer_sk, customer_id, customer_name, country, status | Placeholder (awaiting SAP customer source) |

### Statistical Alert Engine — `fct_sensor_alerts`

References `fct_daily_sensor_kpi` via `{{ ref() }}`. Derives 6 alert categories:

| # | Alert | Primitive | Logic |
|---|-------|-----------|-------|
| 1 | Stability Level | STDDEV | stddev < 5 = STABLE, 5-25 = NORMAL, > 25 = UNSTABLE |
| 2 | Signal Quality | CV (stddev/avg) | CV > 0.3 = NOISY, > 0.5 = VERY_NOISY |
| 3 | Outlier Detection | 2σ rule | max > avg + 2×stddev |
| 4 | Range Violation | 1σ band | outside avg ± stddev (68% expected) |
| 5 | Volatility Trend | LAG(stddev) | > 1.5× prev = DEGRADING, < 0.9× = IMPROVING |
| 6 | Spike Detection | LAG(max) | |delta| > 2×stddev = SPIKE |

---

## Star Schema (Gold Layer)

```
                    ┌──────────────────┐
                    │  dim_equipment   │
                    │────────────────  │
                    │ equipment_sk (PK)│
                    │ equipment_id     │
                    │ equipment_name   │
                    │ equipment_type   │
                    │ location         │
                    │ lifecycle_stage  │
                    └────────┬─────────┘
                             │
                             │ equipment_id
                             │
┌──────────────────┐   ┌─────▼──────────────────┐   ┌──────────────────┐
│   dim_vendor     │   │  fct_daily_sensor_kpi  │   │  dim_customer    │
│────────────────  │   │────────────────────────│   │────────────────  │
│ vendor_sk (PK)   │   │ kpi_sk (PK)           │   │ customer_sk (PK) │
│ vendor_number    │   │ sensor_id (FK)         │   │ customer_id      │
│ vendor_name      │   │ reading_date           │   │ customer_name    │
│ country          │   │ avg_reading            │   │ country          │
│ is_current       │   │ max_reading            │   │ status           │
│ valid_from       │   │ min_reading            │   └──────────────────┘
│ valid_to         │   │ stddev_reading         │
└──────────────────┘   │ total_readings         │
                       │ _gold_loaded_at        │
                       └────────────────────────┘

                       ┌────────────────────────┐
                       │   fct_daily_revenue    │
                       │────────────────────────│
                       │ revenue_sk (PK)        │
                       │ revenue_date           │
                       │ material_group         │
                       │ order_count            │
                       │ total_quantity         │
                       │ total_revenue          │
                       │ _gold_loaded_at        │
                       └────────────────────────┘
```

---

## dbt Custom Macros

| Macro | File | Purpose |
|-------|------|---------|
| `three_gate_validation` | `macros/three_gate_validation.sql` | Reusable 3-gate quality check applied across all Silver models |
| `scd2_merge` | `macros/scd2_merge.sql` | Generic SCD Type 2 merge pattern for any dimension table |
| `audit_columns` | `macros/audit_columns.sql` | Auto-adds created_at, updated_at, loaded_by audit trail columns |
| `generate_schema_name` | `macros/generate_schema_name.sql` | Environment-aware schema routing (dev/staging/prod) |

---

## dbt Tests (Quality Gates)

| Test Type | Example | Layer |
|-----------|---------|-------|
| `unique` | `unique: kpi_sk` | All |
| `not_null` | `not_null: sensor_id` | All |
| `accepted_values` | `quality_flag IN ('PASS','WARN','FAIL')` | Silver |
| `relationships` | `sensor_id references stg_sensor_readings` | Silver → Bronze |

---

## Delta Lake Tables (Databricks)

Used in PySpark notebooks for compute-heavy processing:

| Path | Layer | Format | Key Operations |
|------|-------|--------|----------------|
| `/bronze/sensor_readings` | Bronze | Delta | Auto Loader ingest, schema inference, rescue column |
| `/silver/sensor_cleaned` | Silver | Delta | 3-Gate validation, dedup, derived columns |
| `/gold/daily_sensor_kpi` | Gold | Delta | Window functions, aggregations |

Delta features used: Time Travel, MERGE (SCD1/SCD2), Schema Evolution (`mergeSchema=true`), Change Data Feed, OPTIMIZE + ZORDER.

---

## Snowflake Objects

### Databases & Schemas

| Database | Schema | Purpose |
|----------|--------|---------|
| `PETROVA_DW` | `BRONZE` | Raw ingestion tables (VARIANT + typed) |
| `PETROVA_DW` | `SILVER` | Cleaned, validated, SCD2 tables |
| `PETROVA_DW` | `GOLD` | Star schema dimensions + facts |
| `PETROVA_DW` | `AUDIT` | Pipeline logging, immutability checks |

### Key Snowflake Features

| Feature | DDL Script | Exam Topic |
|---------|-----------|------------|
| Warehouses (sizing, auto-suspend) | `01_warehouse.sql` | Performance & Tuning |
| External Stages + Snowpipe | `06_stages_and_tasks.sql` | Data Loading |
| FLATTEN / LATERAL / PARSE_JSON | `07_semi_structured.sql` | Semi-Structured |
| Streams (change tracking) | `08_streams.sql` | Streams & Tasks |
| Time Travel (AT/BEFORE, UNDROP) | `09_time_travel.sql` | Time Travel |
| Secure Views + Data Sharing | `10_data_sharing_security.sql` | Security |

---

## Security & Governance Model

### RBAC Hierarchy (4 Roles)
```
PETROVA_ADMIN          ← Full DDL, procedure management, RBAC grants
    └── PETROVA_ENGINEER   ← ETL writes (Bronze/Silver), procedure execution
        └── PETROVA_ANALYST    ← Read Bronze/Silver, query Gold
            └── PETROVA_READER     ← Read-only Gold layer (BI tools only)
```

### Security Controls

| Layer | Control | Implementation |
|-------|---------|----------------|
| Network | TLS 1.2+ | All connections encrypted in transit |
| Encryption | AES-256 | Data at rest (ADLS, Snowflake, Key Vault) |
| Authentication | Key-pair + SSO | Snowflake key-pair auth, Azure AD for ADF |
| Secrets | Azure Key Vault | No credentials in code, Docker, or git |
| Masking | Dynamic Data Masking | PII fields masked for ANALYST/READER roles |
| Audit | Pipeline Run Log | Every dbt run, SSIS execution logged to AUDIT schema |
| Immutability | 7 Gold Commandments | No direct writes to Gold, all via MERGE procedures |
| Recovery | Time Travel 90d + Fail-Safe 7d | 97-day total recovery window |
| DR | Zero-Copy Clone | Instant rollback (< 1 second) for Gold tables |
| DevSecOps | CI Gates | SQLFluff + dbt test + security scan before deploy |

### Python Dependency Security

| Package | Purpose | Connection Type |
|---------|---------|-----------------|
| `pyspark` + `delta-spark` | Compute engine | Local / Databricks Runtime |
| `snowflake-connector-python` | Snowflake access | Key-pair auth (no password) |
| `pyodbc` + `sqlalchemy` | SQL Server bridge | Windows auth / encrypted connection |
| `great-expectations` | Data quality | Local execution only |
| `pyarrow` + `pandas` | File I/O | Local filesystem / ADLS |
| `dbt-core` + `dbt-snowflake` | Transformations | Profile-based auth |
| `apache-airflow` | Orchestration | Connections stored in Airflow Fernet-encrypted DB |

---

## 10 Silver Cleaning Logics

| Gate | # | Logic Name | PySpark / SQL Implementation | Exam Relevance |
|------|---|-----------|------------------------------|----------------|
| Gate 1 | 1 | Null Detection | `F.col().isNull()` → quality_flag = FAIL | Databricks: DataFrame API |
| Gate 1 | 2 | Type Casting | `StructType`, `cast()` | Databricks: Schema enforcement |
| Gate 1 | 3 | Range Validation | `F.col().between(lo, hi)` | SnowPro: Data validation |
| Gate 2 | 4 | Status Filtering | `.filter(F.col("status").isin(...))` | dbt: `where` config |
| Gate 2 | 5 | Deduplication | `ROW_NUMBER() OVER(PARTITION BY ...)` | All 3 certs: Window functions |
| Gate 2 | 6 | Derived Columns | `withColumn("reading_date", to_date())` | Databricks: Transformations |
| Gate 2 | 7 | SCD Type 2 | `MERGE ... WHEN MATCHED / NOT MATCHED` | SnowPro: MERGE, dbt: incremental |
| Gate 3 | 8 | Referential Integrity | `JOIN` checks, `relationships` test | dbt: relationship tests |
| Gate 3 | 9 | Aggregation Guards | `HAVING COUNT(*) > 0` | SnowPro: Aggregate functions |
| Gate 3 | 10 | Late-Arriving Data | `withWatermark()`, window functions | Databricks: Structured Streaming |
