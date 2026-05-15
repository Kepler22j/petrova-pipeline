# PETROVA – Incident Response Runbook

## Severity Levels

| Level | Description | Response Time | Escalation |
|-------|-------------|---------------|------------|
| SEV-1 | Gold layer corrupted / pipeline down | 15 min | PagerDuty → On-call → Manager |
| SEV-2 | Gate failure blocking promotion | 30 min | PagerDuty → On-call |
| SEV-3 | Quality warnings / partial failure | 2 hours | Email alert |
| SEV-4 | Non-critical / cosmetic | Next business day | Jira ticket |

## Common Scenarios

### Bronze Gate Failure
1. Check `AUDIT.PIPELINE_LOG` for error details
2. Verify source schema hasn't changed (SAP BODS / Parquet)
3. If schema change: update `_sources.yml` and `stg_*.sql`
4. Re-run: `dbt run --select tag:bronze`

### Silver Gate Failure
1. Check `dbt test` output for specific test failures
2. Review `quality_flag` distribution in Silver tables
3. If data quality issue: investigate source system
4. If threshold too strict: adjust `dbt_expectations` config

### Gold Gate FMEA Block
1. Query `GOLD.V_FMEA_RESULTS` for RPN scores
2. Identify high-risk records
3. If false positive: adjust FMEA threshold in macro
4. If real issue: halt pipeline, notify stakeholders

### Pipeline Timeout
1. Check Airflow UI for stuck tasks
2. Verify Snowflake warehouse isn't suspended
3. Check for query queuing: `SHOW QUERIES`
4. Scale warehouse if needed: `ALTER WAREHOUSE ... SET WAREHOUSE_SIZE = 'LARGE'`

## Rollback Procedure
```sql
-- Zero-Copy Clone rollback (< 1 second)
ALTER TABLE PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI
  SWAP WITH PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI_BACKUP_20250115;
```
