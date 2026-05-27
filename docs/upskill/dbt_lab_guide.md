# dbt Analytics Engineer — Hands-On Lab Guide

**Local Project:** `petrova-pipeline/dbt/`
**dbt Cloud:** `cloud.getdbt.com` (etlpetrova097@gmail.com)
**PETROVA Models:** 5 staging + 5 intermediate + 6 marts = 16 models, 51 tests

---

## Study Plan (2-Week Sprint)

| Day | Lab | Exam Domain | Time |
|-----|-----|------------|------|
| 1 | Lab 1: Project Structure & `dbt run` | dbt Fundamentals | 2 hrs |
| 2 | Lab 2: Models & Materializations | Developing Models | 2 hrs |
| 3 | Lab 3: Sources & Refs | Developing Models | 1.5 hrs |
| 4 | Lab 4: Tests & Data Quality | Testing | 2 hrs |
| 5 | Lab 5: Incremental Models | Developing Models | 2.5 hrs |
| 6 | Lab 6: Seeds, Snapshots & SCD2 | Developing Models | 2 hrs |
| 7 | Lab 7: Macros & Jinja | Developing Models | 2 hrs |
| 8 | Lab 8: Packages & dbt_utils | Developing Models | 1.5 hrs |
| 9 | Lab 9: dbt Cloud IDE Features | Deployment (Cloud) | 2 hrs |
| 10 | Lab 10: Jobs, CI/CD & Documentation | Deployment | 2 hrs |
| 11-12 | Practice Questions + Weak Areas | All Domains | 3 hrs |
| 13-14 | Mock Exam + Final Review | All Domains | 3 hrs |

---

## Lab 1: Project Structure & Running dbt

**Exam Domain:** dbt Fundamentals

### Hands-On Exercises

```bash
# Exercise 1.1: Start Docker environment
cd petrova-pipeline
make docker-up  # Starts PostgreSQL + Airflow + Jupyter

# Exercise 1.2: Run dbt pipeline locally
cd dbt
dbt seed --target dev --profiles-dir .     # Load CSV reference data
dbt run --target dev --profiles-dir .      # Build all 16 models
dbt test --target dev --profiles-dir .     # Run 51 tests — all should PASS

# Exercise 1.3: Explore project structure
# dbt_project.yml   — project config, model paths, vars
# profiles.yml      — connection targets (dev=PostgreSQL, prod=Snowflake)
# models/
#   staging/         — Bronze (5 stg_ models)
#   intermediate/    — Silver (5 int_ / scd2_ models)
#   marts/           — Gold (6 fct_ / dim_ models)
# macros/           — Reusable SQL + Jinja
# seeds/            — CSV reference data
# tests/            — Custom singular tests

# Exercise 1.4: Understand model layers (exam topic)
# staging/    = source-aligned, 1:1 with source tables, minimal transforms
# intermediate/ = business logic, joins, cleaning, SCD2
# marts/      = fact + dimension tables for BI consumption

# Exercise 1.5: Key dbt commands (exam topic)
dbt compile      # Generate SQL without executing (check compiled code)
dbt run          # Build all models
dbt test         # Run all tests
dbt build        # run + test in DAG order (preferred!)
dbt seed         # Load CSV seeds
dbt snapshot     # Run snapshot tables (SCD2)
dbt docs generate  # Generate documentation
dbt docs serve     # Open docs site with lineage graph
```

### Exam Questions to Master

1. **Q:** `dbt run` vs `dbt build`?
   **A:** `dbt run`: only builds models. `dbt build`: run + test + snapshot + seed in DAG order. Tests run after their upstream model.

2. **Q:** What is `dbt compile`?
   **A:** Compiles Jinja+SQL to raw SQL. Outputs to `target/compiled/`. No execution. Useful for debugging.

3. **Q:** Project file hierarchy?
   **A:** `dbt_project.yml` (project), `profiles.yml` (connections), `packages.yml` (dependencies).

---

## Lab 2: Models & Materializations

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```bash
# Exercise 2.1: Examine materializations in PETROVA
# Check each model's config:
```

```sql
-- staging models = views (always fresh from source)
-- File: models/staging/stg_sensor_readings.sql
{{ config(materialized='view', schema='bronze') }}

-- intermediate models = incremental (SCD2) or table
-- File: models/intermediate/scd2_vendors.sql
{{ config(materialized='incremental', schema='silver', unique_key='vendor_sk') }}

-- marts models = table (aggregated, ready for BI)
-- File: models/marts/fct_daily_sensor_kpi.sql
{{ config(materialized='table', schema='gold') }}
```

```bash
# Exercise 2.2: Test materializations
dbt run --select stg_sensor_readings  # Builds as VIEW
dbt run --select fct_daily_sensor_kpi  # Builds as TABLE

# Exercise 2.3: Model selection syntax (exam topic!)
dbt run --select my_model              # Single model
dbt run --select my_model+             # Model + all downstream
dbt run --select +my_model             # All upstream + model
dbt run --select +my_model+            # Full lineage
dbt run --select tag:gold              # All models with tag
dbt run --select staging.*             # All staging models
dbt run --select source:petrova_raw+   # Source + all downstream
dbt run --exclude stg_sap_vendors      # Everything except this

# Exercise 2.4: Full vs incremental refresh
dbt run --select scd2_vendors            # Incremental (only new/changed)
dbt run --select scd2_vendors --full-refresh  # Full rebuild from scratch
```

### Exam Questions to Master

1. **Q:** Four materialization types?
   **A:** `view` (virtual, always fresh), `table` (physical, rebuilt each run), `incremental` (appends/merges new data), `ephemeral` (CTE inlined into downstream).

2. **Q:** When to use incremental?
   **A:** Large tables where rebuilding is expensive. Must define `unique_key` and `is_incremental()` logic.

3. **Q:** What does `ephemeral` do?
   **A:** No database object created. Model is compiled as a CTE and injected into downstream models that `ref()` it.

4. **Q:** `--select` graph operators?
   **A:** `+` (upstream/downstream), `@` (upstream + tests of selected), `tag:`, `source:`, `path:`.

---

## Lab 3: Sources & Refs

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```yaml
# Exercise 3.1: Examine _sources.yml
# File: models/staging/_sources.yml
sources:
  - name: petrova_raw
    schema: raw
    tables:
      - name: sensor_readings
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

```sql
-- Exercise 3.2: source() function (exam topic)
-- In staging models:
SELECT * FROM {{ source('petrova_raw', 'sensor_readings') }}
-- Compiles to: SELECT * FROM raw.sensor_readings

-- Exercise 3.3: ref() function (exam topic)
-- In intermediate/marts models:
SELECT * FROM {{ ref('stg_sensor_readings') }}
-- Compiles to: SELECT * FROM bronze.stg_sensor_readings
-- ref() builds the DAG automatically!
```

```bash
# Exercise 3.4: Source freshness check (exam topic)
dbt source freshness
# Checks: is source data within warn/error thresholds?
# Uses loaded_at_field to determine data age
```

### Exam Questions to Master

1. **Q:** `source()` vs `ref()`?
   **A:** `source()`: references raw/external tables defined in _sources.yml. `ref()`: references other dbt models. Both build the DAG.

2. **Q:** What is source freshness?
   **A:** Checks how recent source data is using `loaded_at_field`. Warns/errors if data is stale.

3. **Q:** Can you `ref()` a source?
   **A:** No. Sources use `source()`. Models use `ref()`. Never hardcode table names.

---

## Lab 4: Tests & Data Quality

**Exam Domain:** Testing (20%)

### Hands-On Exercises

```yaml
# Exercise 4.1: Schema tests (generic tests)
# File: models/staging/_schema.yml
models:
  - name: stg_sensor_readings
    columns:
      - name: sensor_id
        tests:
          - not_null           # Built-in
          - unique             # Built-in
      - name: status
        tests:
          - accepted_values:   # Built-in
              values: ['ACTIVE', 'INACTIVE', 'MAINTENANCE']
      - name: equipment_name
        tests:
          - relationships:     # Built-in (referential integrity)
              to: ref('stg_equipment_master')
              field: equipment_name
```

```sql
-- Exercise 4.2: Custom singular test
-- File: tests/assert_no_negative_readings.sql
SELECT *
FROM {{ ref('int_sensor_cleaned') }}
WHERE reading_value < 0
  AND quality_flag = 'PASS'
-- If this returns ANY rows, the test FAILS

-- Exercise 4.3: Custom generic test (reusable macro)
-- File: macros/test_not_negative.sql
{% test not_negative(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0
{% endtest %}

-- Usage in schema.yml:
-- columns:
--   - name: reading_value
--     tests:
--       - not_negative
```

```bash
# Exercise 4.4: Run tests
dbt test                           # All 51 tests
dbt test --select stg_sensor_readings  # Tests for one model
dbt test --select tag:gold         # Tests for gold models only
dbt test --select test_type:singular  # Only singular tests
dbt test --select test_type:generic   # Only generic tests

# Exercise 4.5: dbt_expectations package (advanced tests)
# Already in PETROVA's packages.yml
# Example: expect_column_values_to_be_between
#   - dbt_expectations.expect_column_values_to_be_between:
#       min_value: -999
#       max_value: 9999
```

### Exam Questions to Master

1. **Q:** Four built-in generic tests?
   **A:** `unique`, `not_null`, `accepted_values`, `relationships`.

2. **Q:** Singular vs Generic test?
   **A:** Singular: standalone SQL file in `tests/`, returns failing rows. Generic: reusable macro, configured in schema.yml.

3. **Q:** Test severity levels?
   **A:** `warn` (warning only) or `error` (fail the run). Set via `config(severity='warn')`.

4. **Q:** What does a test query return?
   **A:** Failing rows. Zero rows = PASS. Any rows = FAIL.

---

## Lab 5: Incremental Models

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```sql
-- Exercise 5.1: Examine PETROVA's incremental model
-- File: models/intermediate/scd2_vendors.sql

{{ config(
    materialized='incremental',
    unique_key='vendor_sk',       -- Used for MERGE
    schema='silver'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_sap_vendors') }}
)

{% if is_incremental() %}
-- Only process NEW or CHANGED records
SELECT h.*
FROM hashed h
LEFT JOIN {{ this }} t
    ON h.vendor_number = t.vendor_number AND t.is_current = TRUE
WHERE t.vendor_number IS NULL       -- New vendor
   OR t.row_hash != h.row_hash     -- Changed vendor
{% else %}
-- Full refresh: load everything
SELECT * FROM hashed
{% endif %}
```

```bash
# Exercise 5.2: Run incremental vs full refresh
dbt run --select scd2_vendors             # Incremental (fast)
dbt run --select scd2_vendors --full-refresh  # Full rebuild (slow but clean)

# Exercise 5.3: Understand the compiled SQL
dbt compile --select scd2_vendors
cat target/compiled/petrova/models/intermediate/scd2_vendors.sql
# See the actual SQL that runs (with/without is_incremental)
```

### Exam Questions to Master

1. **Q:** What is `{{ this }}`?
   **A:** References the current model's table (the already-built version). Only valid inside `is_incremental()` block.

2. **Q:** What does `unique_key` do in incremental?
   **A:** Determines MERGE behavior. Matching rows are UPDATED, non-matching rows are INSERTED.

3. **Q:** When does `is_incremental()` return FALSE?
   **A:** On `--full-refresh`, when table doesn't exist yet, or first run ever.

4. **Q:** Incremental strategies?
   **A:** `append` (just INSERT), `merge` (UPSERT by unique_key), `delete+insert` (delete matching, then insert), `insert_overwrite` (partition-level overwrite).

---

## Lab 6: Seeds, Snapshots & SCD2

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```bash
# Exercise 6.1: Seeds (CSV files loaded as tables)
ls dbt/seeds/
# equipment_master.csv, materials.csv, orders.csv, sensor_readings.csv, vendors.csv

dbt seed  # Loads all CSVs into database tables
# Exam: seeds are for SMALL reference data (lookup tables, codes)
# NOT for large datasets (use sources instead)
```

```yaml
# Exercise 6.2: Snapshot configuration (exam topic)
# File: snapshots/vendor_snapshot.yml (example)
snapshots:
  - name: snap_vendors
    relation: ref('stg_sap_vendors')
    config:
      strategy: check         # or 'timestamp'
      unique_key: vendor_number
      check_cols:
        - vendor_name
        - country
        - payment_terms
      # timestamp strategy would use: updated_at: loaded_at
```

```bash
# Exercise 6.3: Run snapshots
dbt snapshot
# Creates SCD Type 2 history: dbt_valid_from, dbt_valid_to, dbt_scd_id
```

### Exam Questions to Master

1. **Q:** Snapshot strategy: `check` vs `timestamp`?
   **A:** `check`: compares column values (more reliable). `timestamp`: uses updated_at column (more efficient). `check_cols: all` checks every column.

2. **Q:** Snapshot SCD2 columns?
   **A:** `dbt_valid_from`, `dbt_valid_to` (null = current), `dbt_scd_id`, `dbt_updated_at`.

3. **Q:** Seeds vs Sources?
   **A:** Seeds: small CSV files in your dbt project, loaded via `dbt seed`. Sources: external tables you query with `source()`.

4. **Q:** Can you test a snapshot?
   **A:** Yes. Add tests in schema.yml just like any other model.

---

## Lab 7: Macros & Jinja

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```sql
-- Exercise 7.1: Examine PETROVA macros
-- File: macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
-- This ensures schema='gold' writes to 'gold' schema directly

-- Exercise 7.2: Jinja basics (exam topic)
-- {{ }} = expression (output value)
-- {% %} = statement (logic, no output)
-- {# #} = comment

-- Variables
{% set my_list = ['sensor_id', 'reading_value', 'status'] %}
SELECT
    {% for col in my_list %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('stg_sensor_readings') }}

-- Exercise 7.3: Conditional logic
{% if target.name == 'prod' %}
    -- Production: use Snowflake syntax
    SELECT CURRENT_TIMESTAMP()
{% else %}
    -- Dev: use PostgreSQL syntax
    SELECT NOW()
{% endif %}

-- Exercise 7.4: Cross-database macros (exam topic)
-- dbt provides adapter-agnostic macros:
{{ dbt.current_timestamp() }}     -- Works on both PG and Snowflake
{{ dbt.datediff('day', 'col1', 'col2') }}
{{ dbt_utils.generate_surrogate_key(['col1', 'col2']) }}
```

### Exam Questions to Master

1. **Q:** `{{ }}` vs `{% %}`?
   **A:** `{{ }}`: outputs a value (expression). `{% %}`: executes logic (statement, no output).

2. **Q:** What is `{{ this }}`?
   **A:** References the database representation of the current model.

3. **Q:** What is `{{ target }}`?
   **A:** Contains target info: `target.name`, `target.schema`, `target.database`, `target.type`.

4. **Q:** Custom macros location?
   **A:** `macros/` directory. Any `.sql` file with `{% macro name() %}...{% endmacro %}`.

---

## Lab 8: Packages & dbt_utils

**Exam Domain:** Developing dbt Models (40%)

### Hands-On Exercises

```yaml
# Exercise 8.1: PETROVA's packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: "1.1.1"
  - package: calogica/dbt_expectations
    version: "0.10.3"
  - package: dbt-labs/codegen
    version: "0.12.1"
```

```bash
# Exercise 8.2: Install packages
dbt deps  # Downloads packages to dbt_packages/

# Exercise 8.3: Key dbt_utils macros used in PETROVA
```

```sql
-- generate_surrogate_key (used everywhere)
{{ dbt_utils.generate_surrogate_key(['sensor_id', 'kpi_date']) }}

-- star (select all columns from a relation)
{{ dbt_utils.star(from=ref('stg_sensor_readings'), except=['_loaded_at']) }}

-- date_spine (generate date range)
{{ dbt_utils.date_spine(datepart="day", start_date="'2024-01-01'", end_date="'2025-12-31'") }}
```

### Exam Questions to Master

1. **Q:** How to install packages?
   **A:** Define in `packages.yml`, run `dbt deps`. Installed to `dbt_packages/`.

2. **Q:** `dbt_utils.generate_surrogate_key` — what does it create?
   **A:** MD5 hash of concatenated columns. Deterministic unique key.

3. **Q:** Where does `packages.yml` live?
   **A:** Project root, same level as `dbt_project.yml`.

---

## Lab 9: dbt Cloud IDE Features

**Exam Domain:** Deployment (20%)

### Hands-On Exercises (do these in dbt Cloud at cloud.getdbt.com)

1. **Explore the Cloud IDE:**
   - Open your PETROVA project
   - Click on a model → see the **Lineage** tab (DAG visualization)
   - Click **Compile** → see the **Compiled Code** tab (Jinja → SQL)
   - Use the **Command Bar** at bottom: `dbt run --select fct_daily_sensor_kpi+`

2. **Model selection in command bar (exam topic):**
   ```
   dbt run --select +fct_sensor_alerts    # Upstream + model
   dbt run --select tag:gold              # By tag
   dbt run --select source:petrova_raw+   # Source + downstream
   dbt build --select 1+fct_sensor_alerts # 1 level upstream only
   ```

3. **Version control in Cloud IDE:**
   - Create branch → make changes → commit → create PR
   - Exam tests this flow!

4. **Defer to production (exam topic):**
   - In development, unmodified models read from PROD tables
   - Only changed models are built in dev
   - Saves compute: `dbt run --select state:modified --defer --state prod-run-artifacts`

### Exam Questions to Master

1. **Q:** What does "defer to production" do?
   **A:** Unmodified models reference production tables instead of rebuilding in dev. Only changed models are built.

2. **Q:** Slim CI?
   **A:** CI job that only tests modified models + downstream. Uses `state:modified` selector.

3. **Q:** dbt Cloud environments?
   **A:** Development (IDE), Staging (CI/PR), Production (scheduled jobs). Each has its own target/credentials.

---

## Lab 10: Jobs, CI/CD & Documentation

**Exam Domain:** Deployment (20%)

### Hands-On Exercises

```bash
# Exercise 10.1: Documentation (exam topic)
dbt docs generate   # Creates catalog.json + manifest.json
dbt docs serve      # Opens browser with lineage graph

# Exercise 10.2: Exposures (what uses your models)
# File: models/marts/_exposures.yml
# Defines downstream consumers: dashboards, reports, APIs
# Shows in lineage graph as orange nodes
```

**In dbt Cloud:**

1. **Create a scheduled job:**
   - Deploy → Jobs → Create Job
   - Commands: `dbt build --target prod`
   - Schedule: Daily at 04:00 UTC
   - Trigger on: schedule + PR merge

2. **CI Job (Slim CI):**
   - Triggered on PR
   - Command: `dbt build --select state:modified+ --defer --state prod-run-artifacts`
   - Only tests changed models + downstream

### Exam Questions to Master

1. **Q:** `dbt docs generate` creates what files?
   **A:** `target/catalog.json` (column-level metadata) + `target/manifest.json` (project DAG).

2. **Q:** What are exposures?
   **A:** Declarations of downstream consumers (dashboards, apps). Appear in lineage graph. Don't affect model execution.

3. **Q:** CI job trigger?
   **A:** Triggered by PR (pull request). Runs modified models + tests against prod state.

---

## Cheat Sheet — dbt Analytics Engineer Quick Reference

### Exam Domain Weights
| Domain | Weight | Your PETROVA Coverage |
|--------|--------|----------------------|
| Developing dbt Models | 40% | 16 models, macros, packages, SCD2 |
| Testing | 20% | 51 tests, dbt_expectations |
| Deployment | 20% | Cloud IDE, jobs, CI/CD |
| dbt Fundamentals | 20% | Project structure, commands, DAG |

### Critical Commands

```bash
dbt run                    # Build all models
dbt test                   # Run all tests
dbt build                  # run + test + snapshot + seed (DAG order)
dbt seed                   # Load CSV seeds
dbt snapshot               # Run snapshots (SCD2)
dbt compile                # Generate SQL without executing
dbt docs generate          # Create documentation
dbt docs serve             # Open docs site with lineage
dbt source freshness       # Check source data age
dbt deps                   # Install packages
dbt run --full-refresh     # Rebuild incremental models from scratch
dbt run --select tag:gold  # Select by tag
dbt run --select +model+   # Full lineage of a model
```

### Materializations

| Type | Creates | When to Use |
|------|---------|-------------|
| `view` | SQL view | Small tables, always-fresh staging |
| `table` | Physical table | Marts, aggregated data |
| `incremental` | Append/merge | Large tables, SCD2, frequent updates |
| `ephemeral` | CTE (no object) | Helper logic, reused in multiple models |

### PETROVA Model → Exam Topic Mapping

| PETROVA Model | Exam Topic |
|---------------|-----------|
| `stg_*` (5 models) | Sources, staging pattern, `source()` |
| `int_sensor_cleaned` | Filters, column transforms, quality flags |
| `int_orders_validated` | Joins, deduplication, derived columns |
| `scd2_vendors` | Incremental, `is_incremental()`, `{{ this }}`, SCD2 |
| `fct_daily_sensor_kpi` | Aggregations, window functions, `table` materialization |
| `fct_sensor_alerts` | Complex SQL, CASE, LAG, composite logic |
| `dim_*` (3 models) | Dimension modeling, surrogate keys |
| `_schema.yml` tests | Generic tests, `not_null`, `unique`, `relationships` |
| `_sources.yml` | Source definitions, freshness |
| `_exposures.yml` | Exposures, lineage documentation |
| `generate_schema_name` | Custom macros, `target` context |
| `packages.yml` | Package management, dbt_utils |

### Key Jinja Patterns

```sql
{{ ref('model_name') }}              -- Reference another model
{{ source('source', 'table') }}      -- Reference source table
{{ config(materialized='table') }}   -- Model configuration
{{ this }}                           -- Current model's table
{{ target.name }}                    -- Current target (dev/prod)
{% if is_incremental() %}            -- Incremental logic
{{ dbt.current_timestamp() }}        -- Cross-DB timestamp
{{ dbt_utils.generate_surrogate_key(['col1']) }}  -- Hash key

-- Loop
{% for item in list %} ... {% endfor %}
-- Conditional
{% if condition %} ... {% elif %} ... {% else %} ... {% endif %}
-- Set variable
{% set var = value %}
```
