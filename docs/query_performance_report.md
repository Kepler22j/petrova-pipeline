# PETROVA — Query Performance & Workflow Validation Report

**Author:** Jay Pechnarai | **Date:** 2026-05-21 | **Platform:** Databricks + Snowflake + dbt Core 1.8.7

---

## 1. Executive Summary

This report validates the query workflow, SQL correctness, and performance optimization patterns across all 22 SQL files in the PETROVA dbt project. The validation covers 5 staging models (Bronze), 5 intermediate models (Silver), 6 mart models (Gold), 4 macros, and 2 snapshots.

**Result:** 7 issues identified and resolved. All queries now follow consistent column naming, proper join logic, and performance best practices.

---

## 2. Model Inventory & Materialization Strategy

| Layer | Model | Materialization | Incremental | Performance Pattern |
|-------|-------|----------------|-------------|---------------------|
| Bronze | stg_sensor_readings | view | — | Zero-copy, no storage cost |
| Bronze | stg_sap_orders | view | — | Zero-copy, no storage cost |
| Bronze | stg_sap_materials | view | — | Zero-copy, no storage cost |
| Bronze | stg_sap_vendors | view | — | Zero-copy, no storage cost |
| Bronze | stg_equipment_master | view | — | Zero-copy, no storage cost |
| Silver | int_sensor_cleaned | incremental | `loaded_at > MAX(loaded_at)` | Partition pruning on timestamp |
| Silver | int_orders_validated | incremental | `loaded_at > MAX(loaded_at)` | ROW_NUMBER dedup + LEFT JOIN |
| Silver | int_equipment_enriched | incremental | `loaded_at > MAX(loaded_at)` | DATEDIFF derived columns |
| Silver | int_order_line_items | ephemeral | — | Compiled inline as CTE |
| Silver | scd2_vendors | incremental | Hash-based change detection | SCD Type 2 MERGE pattern |
| Gold | fct_daily_sensor_kpi | table | — | Full aggregation, surrogate keys |
| Gold | fct_sensor_alerts | table | — | Window functions (LAG), SPC engine |
| Gold | fct_daily_revenue | table | — | GROUP BY aggregation with COALESCE |
| Gold | dim_equipment | table | — | Pass-through from Silver |
| Gold | dim_vendor | table | — | SCD2 current + history |
| Gold | dim_customer | table | — | Placeholder (future source) |

**Optimization Score:** Bronze uses views (zero cost), Silver uses incremental (processes only new data), Gold uses tables (pre-computed for BI tools). This is the optimal materialization strategy for a 400K+ records/day pipeline.

---

## 3. Issues Found & Resolved

### Issue 1: Column Name Mismatch in int_orders_validated (CRITICAL)
- **Problem:** Model referenced `order_number` and `material_number` but staging model uses `order_id` and `material_id`
- **Impact:** Query would fail at compile time — dbt run would error
- **Fix:** Rewrote model to use correct column names, added ROW_NUMBER dedup, and joined on `equipment_id` to equipment master

### Issue 2: Missing `status` Column in stg_equipment_master (HIGH)
- **Problem:** `int_equipment_enriched` references `status` column but staging model didn't select it
- **Impact:** Compiled SQL would fail — `status` not found in source CTE
- **Fix:** Added `status` to SELECT in stg_equipment_master

### Issue 3: Missing Columns in stg_sap_orders (HIGH)
- **Problem:** Downstream models reference `material_id`, `order_quantity`, `net_value` but staging only had 8 columns
- **Impact:** int_order_line_items and fct_daily_revenue would fail
- **Fix:** Added `material_id`, `order_quantity`, `net_value` to staging SELECT

### Issue 4: vendor_id vs vendor_number Mismatch (MEDIUM)
- **Problem:** `scd2_vendors` references `vendor_number` but staging uses `vendor_id`
- **Impact:** SCD2 model would compile but match zero rows on join
- **Fix:** Added `vendor_id AS vendor_number` alias in stg_sap_vendors

### Issue 5: Invalid Jinja Filter in scd2_merge Macro (MEDIUM)
- **Problem:** `tracked_columns | map('prefix', 'src.')` is not a valid Jinja2 filter
- **Impact:** Macro would throw Jinja compilation error if invoked
- **Fix:** Replaced with explicit `{% for col in tracked_columns %}src.{{ col }}{% endfor %}` loop. Also fixed `CONCAT` to `CONCAT_WS('|', ...)` for proper hash generation

### Issue 6: material_group Not in Source (LOW)
- **Problem:** `fct_daily_revenue` grouped by `material_group` but no such column in source
- **Impact:** Would fail at query execution
- **Fix:** Added `material_type AS material_group` alias in stg_sap_materials; rewrote fct_daily_revenue to group by `equipment_type` + `equipment_location`

### Issue 7: Division Safety in fct_daily_revenue (LOW)
- **Problem:** `AVG(net_value)` could return misleading results when net_value contains zeros
- **Impact:** Incorrect business metrics
- **Fix:** Changed to `AVG(NULLIF(net_value, 0))` and wrapped SUM with COALESCE

---

## 4. Performance Optimization Patterns

### 4.1 Incremental Processing (Silver Layer)
All Silver models use the `is_incremental()` pattern with timestamp-based filtering:
```sql
{% if is_incremental() %}
WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
```
**Impact:** At 400K records/day, this reduces daily processing from full-table scan to ~400K rows instead of millions of accumulated historical rows.

### 4.2 Surrogate Key Generation
All models use `dbt_utils.generate_surrogate_key()` for deterministic, collision-resistant identifiers:
```sql
{{ dbt_utils.generate_surrogate_key(['sensor_id', 'reading_timestamp']) }}
```
**Impact:** Enables reliable deduplication and incremental MERGE operations without relying on natural keys.

### 4.3 Window Functions for SPC Engine
The `fct_sensor_alerts` model uses `LAG()` with `PARTITION BY sensor_id ORDER BY kpi_date`:
```sql
LAG(stddev_reading, 1) OVER (PARTITION BY sensor_id ORDER BY kpi_date) AS prev_stddev
```
**Impact:** Single-pass computation of trend, volatility, and spike detection. No self-joins required — O(n) instead of O(n²).

### 4.4 NULLIF Division Safety
All division operations use `NULLIF` to prevent divide-by-zero:
```sql
stddev_reading / NULLIF(ABS(avg_reading), 0)
net_value / NULLIF(order_quantity, 0)
```
**Impact:** Prevents query failures and NULL propagation in aggregation pipelines.

### 4.5 COALESCE Null Handling
Aggregations wrap nullable columns with COALESCE:
```sql
SUM(COALESCE(net_value, 0))
```
**Impact:** Ensures consistent aggregation results even when source data has NULL values.

### 4.6 ROW_NUMBER Deduplication
Silver layer uses ROW_NUMBER for deterministic dedup:
```sql
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY loaded_at DESC) AS _rn
...
WHERE _rn = 1
```
**Impact:** Guaranteed single-row-per-key output without GROUP BY limitations.

### 4.7 SCD Type 2 Hash-Based Change Detection
The scd2_vendors model uses hash comparison for efficient change detection:
```sql
{{ dbt_utils.generate_surrogate_key(['vendor_name', 'country', 'payment_terms']) }} AS row_hash
...
WHERE t.row_hash != h.row_hash  -- only process changed rows
```
**Impact:** Only inserts new SCD2 rows when tracked columns actually change. Prevents unnecessary row proliferation.

### 4.8 Ephemeral Model for CTE Inlining
`int_order_line_items` uses `materialized='ephemeral'` to avoid creating a database object:
```sql
{{ config(materialized='ephemeral') }}
```
**Impact:** Zero storage cost, compiled inline as CTE in downstream models. Useful for shared intermediate logic.

---

## 5. Query Execution Flow (DAG Order)

```
Layer  Model                    Depends On                   Gate
─────  ─────────────────────    ──────────────────────────   ────
  B    stg_sensor_readings      source: sensor_readings       —
  B    stg_sap_orders           source: raw_sap_orders        —
  B    stg_sap_materials        source: raw_sap_materials     —
  B    stg_sap_vendors          source: raw_sap_vendors       —
  B    stg_equipment_master     source: raw_equipment_master  —
  S    int_sensor_cleaned       stg_sensor_readings           1,2,3
  S    int_equipment_enriched   stg_equipment_master          2,6
  S    int_orders_validated     stg_sap_orders + stg_equip    1,2,3,5,8
  S    int_order_line_items     stg_sap_orders                2,6
  S    scd2_vendors             stg_sap_vendors               7
  G    fct_daily_sensor_kpi     int_sensor_cleaned            —
  G    fct_sensor_alerts        fct_daily_sensor_kpi          SPC
  G    fct_daily_revenue        int_orders_validated          —
  G    dim_equipment            int_equipment_enriched        —
  G    dim_vendor               scd2_vendors                  —
  G    dim_customer             (standalone placeholder)      —
```

**Critical Path:** `stg_sensor_readings → int_sensor_cleaned → fct_daily_sensor_kpi → fct_sensor_alerts`

This is the longest dependency chain (4 hops) and processes the highest volume (400K+ records/day). It should be prioritized in Airflow scheduling.

---

## 6. Snowflake-Specific Optimization Recommendations

| Optimization | Current | Recommended | Impact |
|-------------|---------|-------------|--------|
| Clustering | None | `CLUSTER BY (sensor_id, kpi_date)` on fct_daily_sensor_kpi | 60-80% scan reduction for time-series queries |
| Warehouse sizing | Default | XS for Bronze/Silver, S for Gold aggregations | Cost optimization |
| Result caching | Default ON | Leverage for repeated BI queries | Near-zero latency on repeat queries |
| Search optimization | Off | Enable on `fct_sensor_alerts.alert_severity` | Faster point lookups for CRITICAL alerts |
| Micro-partition pruning | Automatic | Ensure `kpi_date` in WHERE clauses | Snowflake auto-prunes on sorted columns |

---

## 7. Databricks-Specific Optimization Recommendations

| Optimization | Current | Recommended | Impact |
|-------------|---------|-------------|--------|
| Delta OPTIMIZE | In notebook | Schedule weekly via Airflow | Compacts small files from streaming |
| ZORDER | `sensor_id, reading_timestamp` | Add to all Silver tables | Co-locates related data for faster reads |
| Photon | Enabled (Runtime 15.4 LTS) | Keep enabled | 2-5x faster for aggregations |
| Auto-scaling | Default | Min 1, Max 4 workers | Balance cost vs throughput |
| Caching | Default | Enable Delta caching on driver | Faster iterative development |

---

## 8. Test Coverage Summary

| Layer | Tests | Types |
|-------|-------|-------|
| Bronze | 11 | not_null, unique |
| Silver | 5 (schema.yml) | not_null, unique, accepted_values |
| Gold | 12 | not_null, unique, accepted_values (alert_severity, stability_level, signal_quality, volatility_trend, spike_status) |
| **Total** | **28** | — |

**Recommendation:** Add `relationships` tests between Silver and Gold to validate referential integrity (Gate 3 pattern).

---

## 9. Conclusion

The PETROVA dbt project follows production-grade patterns: incremental processing at Silver, surrogate keys throughout, window functions for SPC, and proper null-safety. The 7 issues found were column-alignment bugs that would have surfaced at compile time — all are now resolved.

The query workflow is clean, the DAG has no circular dependencies, and the materialization strategy is optimized for a 400K+ records/day workload.
