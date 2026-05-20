{% docs petrova_overview %}

# PETROVA Data Platform

This dbt project implements a **Medallion Architecture** (Bronze → Silver → Gold)
for the PETROVA hybrid cloud data platform.

## Architecture Layers

| Layer | Schema | Purpose | Materialization |
|-------|--------|---------|-----------------|
| Bronze | `staging/` | Raw data, schema validation | View |
| Silver | `intermediate/` | Cleaned, enriched, SCD2 | Incremental |
| Gold | `marts/` | Business KPIs, dimensions | Table |

## Key Patterns

- **3-Gate Validation**: Bronze Gate (schema) → Silver Gate (quality) → Gold Gate (FMEA)
- **SCD Type 2**: Full history tracking via snapshots and custom `scd2_merge` macro
- **Gold Immutability**: MERGE-only writes, RBAC enforcement, Zero-Copy Clone backup

{% enddocs %}

{% docs quality_flag %}

Quality flag assigned during Silver Gate validation.

| Value | Meaning |
|-------|---------|
| `PASS` | Record passed all quality checks |
| `WARN` | Record has minor quality issues but is usable |
| `FAIL` | Record failed validation and should be excluded |

{% enddocs %}

{% docs sensor_reading_sk %}

Surrogate key generated from `sensor_id` and `reading_timestamp`
using `dbt_utils.generate_surrogate_key()`. This provides a
deterministic, collision-resistant identifier for deduplication
and incremental loading.

{% enddocs %}

{% docs is_current %}

SCD Type 2 currency flag.

- `TRUE`: This is the current/active version of the record
- `FALSE`: This is a historical version that has been superseded

Used in conjunction with `valid_from` and `valid_to` timestamps
to reconstruct point-in-time state.

{% enddocs %}
