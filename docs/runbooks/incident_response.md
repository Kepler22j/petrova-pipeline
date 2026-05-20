# PETROVA – Incident Response Runbook

## Severity Levels

| Level | Description | Response Time | Escalation | Alert Engine Trigger |
|-------|-------------|---------------|------------|---------------------|
| SEV-1 | Gold layer corrupted / pipeline down / CRITICAL alert | 15 min | PagerDuty → On-call → Manager | `alert_severity = 'CRITICAL'` (UNSTABLE + NOISY) |
| SEV-2 | Gate failure blocking promotion / WARNING alerts | 30 min | PagerDuty → On-call | `alert_severity = 'WARNING'` |
| SEV-3 | Quality warnings / partial failure / degrading trend | 2 hours | Email alert | `volatility_trend = 'DEGRADING'` |
| SEV-4 | Non-critical / cosmetic / improving trends | Next business day | Jira ticket | Informational only |

---

## SPC Alert Engine Scenarios

### CRITICAL: Sensor UNSTABLE + NOISY (SEV-1)
**Trigger:** `stability_level = 'UNSTABLE'` AND `signal_quality IN ('NOISY', 'VERY_NOISY')`

1. Query `fct_sensor_alerts` for affected sensor:
   ```sql
   SELECT sensor_id, kpi_date, stability_level, signal_quality,
          coefficient_of_variation, alert_severity
   FROM GOLD.FCT_SENSOR_ALERTS
   WHERE alert_severity = 'CRITICAL'
     AND kpi_date = CURRENT_DATE();
   ```
2. Check `dim_equipment` for equipment context (lifecycle_stage, location)
3. If `volatility_trend = 'DEGRADING'`: escalate to equipment maintenance team
4. If `spike_status = 'SPIKE'`: check for physical event (power surge, valve change)
5. If sensor is on critical equipment (offshore rig): trigger maintenance work order

### WARNING: Spike Detected (SEV-2)
**Trigger:** `spike_status = 'SPIKE'` — sudden jump in max reading exceeding 2× stddev

1. Query alert history for the sensor over last 7 days:
   ```sql
   SELECT kpi_date, avg_reading, max_reading, prev_max, spike_status
   FROM GOLD.FCT_SENSOR_ALERTS
   WHERE sensor_id = '<affected_sensor>'
     AND kpi_date >= CURRENT_DATE() - 7
   ORDER BY kpi_date;
   ```
2. If single spike → likely transient. Monitor next cycle.
3. If repeated spikes → equipment degradation. Escalate to SEV-1.
4. Check `has_outlier` — if TRUE alongside spike, readings are outside 2σ band.

### WARNING: Volatility Degrading (SEV-3)
**Trigger:** `volatility_trend = 'DEGRADING'` — stddev increasing >1.5× vs previous day

1. Compare current vs previous stddev values in `fct_sensor_alerts`
2. If degrading for 3+ consecutive days → escalate to SEV-2
3. Check if correlated with other sensors on same equipment
4. Review `range_violated` — widening range confirms instability

---

## Pipeline Failure Scenarios

### Bronze Layer: Ingestion Failure
1. Check `AUDIT.PIPELINE_LOG` for error details
2. Verify source schema hasn't changed (SAP BODS / Parquet / Auto Loader)
3. Bronze stores raw data only — no filtering, no quality flags
4. If schema change: update `_sources.yml` and `stg_*.sql`
5. If Auto Loader: check `cloudFiles.schemaEvolutionMode` and rescue column
6. Re-run: `dbt run --select tag:bronze`

### Silver Layer: Gate 1/2 Failure
1. Check `dbt test` output for specific test failures
2. Review `quality_flag` distribution:
   ```sql
   SELECT quality_flag, COUNT(*) 
   FROM SILVER.INT_SENSOR_CLEANED 
   GROUP BY quality_flag;
   ```
3. If FAIL rate > 10%: investigate source system data quality
4. Check 10 cleaning logics sequentially:
   - Logic 1 (Null Detection): Are new null columns appearing?
   - Logic 2 (Type Casting): Schema change in source?
   - Logic 3 (Range Validation): Sensor recalibration needed?
   - Logic 4 (Status Filtering): New status values in source?
   - Logic 5 (Deduplication): Duplicate ingestion from BODS?
   - Logic 6 (Derived Columns): Date parsing failures?
   - Logic 7 (SCD2): Hash collision or merge conflict?
5. If threshold too strict: adjust in `three_gate_validation` macro

### Silver Layer: Gate 3 Failure
1. Check referential integrity tests (Logic 8)
2. Verify aggregation guards (Logic 9): `HAVING COUNT(*) > 0`
3. Late-arriving data (Logic 10): Check watermark window settings
4. If `scd2_vendors` merge fails: check `row_hash` computation

### Gold Layer: Immutability Violation
1. Check `sp_gold_immutability_check` procedure output
2. Query `AUDIT.PIPELINE_LOG` for unauthorized write attempts
3. If direct INSERT/UPDATE attempted: RBAC misconfiguration → revoke and re-grant
4. All Gold writes MUST go through MERGE procedures only (7 Commandments)

### Pipeline Timeout
1. Check Airflow UI for stuck tasks
2. Verify Snowflake warehouse isn't suspended: `SHOW WAREHOUSES`
3. Check for query queuing: `SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE EXECUTION_STATUS = 'RUNNING'`
4. Scale warehouse if needed: `ALTER WAREHOUSE PETROVA_WH SET WAREHOUSE_SIZE = 'LARGE'`
5. Check Databricks cluster auto-scaling — is it hitting max workers?

---

## Rollback Procedures

### Gold Table Rollback (Zero-Copy Clone — < 1 second)
```sql
-- Create backup clone before any risky operation
CREATE TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI_BACKUP
  CLONE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI;

-- Rollback via SWAP
ALTER TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI
  SWAP WITH PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI_BACKUP;
```

### Time Travel Recovery (up to 90 days)
```sql
-- Query data as it was at a specific point
SELECT * FROM PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI
  AT(TIMESTAMP => '2025-01-15 08:00:00'::TIMESTAMP);

-- Restore table to previous state
CREATE OR REPLACE TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI
  CLONE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI
  AT(TIMESTAMP => '2025-01-15 08:00:00'::TIMESTAMP);

-- UNDROP a dropped table (within retention)
UNDROP TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI;
```

### Delta Lake Rollback (Databricks)
```sql
-- Restore Delta table to previous version
RESTORE TABLE petrova_prod.silver.sensor_readings_cleaned TO VERSION AS OF 5;

-- Or by timestamp
RESTORE TABLE petrova_prod.silver.sensor_readings_cleaned
  TO TIMESTAMP AS OF '2025-01-15 08:00:00';
```

---

## Security Incident Response

| Scenario | Action | Tool |
|----------|--------|------|
| Credential exposure in git | Rotate key in Azure Key Vault immediately | Key Vault |
| Unauthorized Gold write | Check RBAC grants, revoke access, audit trail | Snowflake RBAC |
| PII data leak | Dynamic masking verification, check READER role access | Masking Policy |
| Failed authentication | Check key-pair rotation schedule, verify Azure AD SSO | Key Vault + AD |
| CI pipeline compromised | Review `.github/workflows/ci.yml`, check SQLFluff + dbt test gates | GitHub Actions |

---

## Recovery Window Summary

| Mechanism | Window | Scope |
|-----------|--------|-------|
| Time Travel | 90 days | Any Snowflake table |
| Fail-Safe | +7 days (after Time Travel) | Snowflake managed recovery |
| Zero-Copy Clone | Instant (<1s) | Pre-created backups |
| Delta RESTORE | Based on VACUUM retention | Delta Lake tables |
| **Total Snowflake** | **97 days** | Full recovery capability |
