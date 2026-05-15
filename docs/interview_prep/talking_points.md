# PETROVA 300K – Interview Talking Points

## Elevator Pitch (30 seconds)
"I built PETROVA 300K, a hybrid cloud data platform that bridges on-premises SAP/SSIS systems with modern cloud services like Snowflake, Databricks, and Azure Data Factory. It features a Medallion Architecture with 3-gate validation, SCD Type 2 tracking, and FMEA-based quality gates — processing 24 pipeline steps across 9 architecture layers."

## Key Differentiators
1. **Triple Orchestration** – Airflow + ADF + SSIS working together (rare in industry)
2. **Gold Immutability** – 7 Commandments enforcing data trust via MERGE-only writes + RBAC
3. **3-Gate Validation** – Bronze (schema) → Silver (quality) → Gold (FMEA business rules)
4. **SCD Type 2** – Full history via SSIS Lookup + Snowflake MERGE (legacy + modern hybrid)
5. **25+ Technologies** – Full-stack data engineering, not just one tool

## Common Questions & Answers

### "Why hybrid instead of pure cloud?"
"Real enterprises have legacy systems. PETROVA demonstrates I can bridge both worlds — SAP BODS and SSIS aren't going away at Fortune 500 companies. The triple orchestration pattern shows I can integrate, not just greenfield."

### "How do you ensure data quality?"
"Three gates: Bronze validates schema completeness, Silver applies statistical quality rules via dbt_expectations and Great Expectations, Gold uses FMEA risk scoring to block high-risk records from production. Each gate logs to an audit trail."

### "Walk me through the SCD2 implementation."
"The vendor dimension uses SCD Type 2 for full history tracking. SSIS Lookup Transform detects changes by comparing hash values. Changed records get expired (valid_to = now, is_current = false) and a new row is inserted. In Snowflake, this is implemented via a MERGE statement in a stored procedure."

### "How do you handle failures?"
"PagerDuty integration with severity-based routing. Each gate can independently alert. Gold has Zero-Copy Clone for instant rollback — less than 1 second to restore. The Airflow DAG uses BranchPythonOperator to route failures to alert tasks."

## Metrics to Mention
- 9 architecture layers, 24 pipeline steps
- 3-gate validation (Bronze/Silver/Gold)
- 4-role RBAC hierarchy
- 7 Gold Immutability Commandments
- SCD Type 2 with full history tracking
- Zero-Copy Clone for sub-second rollback
