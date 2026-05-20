# PETROVA 300K – Interview Talking Points

## Elevator Pitch (30 seconds)

"I built PETROVA 300K, a hybrid cloud data platform that bridges on-premises SAP/SSIS systems with modern cloud — Snowflake, Databricks, and Azure Data Factory. It features a Medallion Architecture with 3-gate validation across 10 silver cleaning logics, an SPC-based statistical alert engine, SCD Type 2 tracking, and traffic-light priority routing — processing 24 pipeline steps across 10 architecture layers with enterprise-grade security."

## Key Differentiators

1. **Statistical Process Control (SPC) Alert Engine** – 6 alert categories derived from 3 primitives (STDDEV, LAG, THRESHOLD). Same math as Shewhart (1924) manufacturing quality. Composite severity routing: CRITICAL → WARNING → OK.
2. **10 Silver Cleaning Logics** – Not just "clean the data." Three gates with 10 named logics: null detection, type casting, range validation, status filtering, deduplication, derived columns, SCD2, referential integrity, aggregation guards, late-arriving data handling.
3. **Triple Orchestration** – Airflow + ADF + SSIS working together (rare in industry). Traffic-light priority: RED = critical pipeline, YELLOW = quality gates, GREEN = analytics/monitoring.
4. **Gold Immutability** – 7 Commandments enforcing data trust via MERGE-only writes + 4-role RBAC hierarchy.
5. **Enterprise Security Posture** – TLS 1.2+, AES-256 encryption, Azure Key Vault, dynamic data masking, 97-day recovery window (90d Time Travel + 7d Fail-Safe), Unity Catalog governance.
6. **25+ Technologies, 11 Python Libraries** – Full-stack data engineering with pyspark, delta-spark, dbt-core, great-expectations, snowflake-connector-python, pandas, pyarrow, apache-airflow, streamlit, plotly, sqlalchemy.

## PTTEP APM Platform — Real-World Anchor

Use this to ground every answer in production experience:

- "At PTTEP, I designed the Asset Performance Management platform for offshore oil rigs — 400,000+ sensor records per day, 1,200+ automated alerts, 99.5% SLA."
- "Predictive maintenance isn't theoretical for me. When a compressor on an offshore platform shows volatility trending DEGRADING, you don't wait — you trigger maintenance before it fails. That's what PETROVA's alert engine models."
- "The SPC engine in PETROVA is directly inspired by the real-time monitoring I built at PTTEP. The difference is PETROVA makes it reproducible and certifiable."

## Common Questions & Answers

### "Why hybrid instead of pure cloud?"
"Real enterprises have legacy systems. PETROVA demonstrates I can bridge both worlds — SAP BODS and SSIS aren't going away at Fortune 500 companies. At PTTEP and NXP, I worked with SAP ERP alongside modern cloud services. The triple orchestration pattern shows I can integrate, not just greenfield."

### "How do you ensure data quality?"
"Three gates with 10 named cleaning logics. Gate 1 (Bronze→Silver) catches nulls, type mismatches, and range violations. Gate 2 (Silver transforms) applies status filtering, deduplication, derived columns, and SCD2 tracking. Gate 3 (Silver→Gold) enforces referential integrity, aggregation guards, and handles late-arriving data. Every record gets a quality_flag — PASS, WARN, or FAIL — and only PASS/WARN records reach Gold."

### "Walk me through the alert engine."
"It's Statistical Process Control — 3 primitives, 6 alerts. STDDEV gives you stability level and signal quality. LAG comparison gives you volatility trend and spike detection. THRESHOLD gives you outlier flagging and range violation. These combine into a composite severity: if a sensor is UNSTABLE AND noisy (CV > 0.3), it's CRITICAL. One unstable indicator alone is WARNING. Everything else is OK. At PTTEP, this kind of logic prevented equipment failures on offshore rigs."

### "Walk me through the SCD2 implementation."
"The vendor dimension uses SCD Type 2 for full history tracking. In dbt, I use an incremental model with row_hash comparison — changed records get expired (valid_to = now, is_current = false) and a new row is inserted. In Snowflake, this maps to a MERGE statement. The macro is reusable for any dimension."

### "How do you handle failures?"
"Severity-based routing with composite alerting. CRITICAL alerts (UNSTABLE + NOISY) trigger immediate PagerDuty escalation. WARNING alerts queue for next-shift review. Gold has Zero-Copy Clone for instant rollback — less than 1 second to restore. The Airflow DAG uses BranchPythonOperator to route failures to alert tasks. Plus 97-day recovery window: 90 days Time Travel + 7 days Fail-Safe."

### "What about security?"
"Defense in depth. Network: TLS 1.2+. Storage: AES-256 at rest across ADLS, Snowflake, and Key Vault. Auth: key-pair + Azure AD SSO — no passwords in code. Authorization: 4-role RBAC (ADMIN → ENGINEER → ANALYST → READER). Data protection: dynamic masking on PII for ANALYST/READER roles. Governance: Unity Catalog for Databricks, audit logging to AUDIT schema. DevSecOps: SQLFluff + dbt test + security scan in CI before deploy."

### "Why Databricks AND Snowflake?"
"Different strengths. Databricks with Delta Lake handles compute-heavy processing — streaming ingestion via Auto Loader, PySpark transformations, OPTIMIZE/Z-ORDER for performance. Snowflake handles the analytical warehouse — star schema, secure views, data sharing, Time Travel. In production at PTTEP, we used a similar split: heavy compute on one platform, analytical queries on another."

## Metrics to Mention

- 10 architecture layers, 24 pipeline steps
- 10 Silver cleaning logics across 3 gates
- 6 SPC alert categories from 3 statistical primitives
- 4-role RBAC hierarchy (ADMIN → ENGINEER → ANALYST → READER)
- 7 Gold Immutability Commandments
- 97-day recovery window (Time Travel + Fail-Safe)
- 11 Python libraries in production ecosystem
- Traffic-light priority routing (RED/YELLOW/GREEN)
- Triple certification target: Databricks DE Associate, SnowPro Core, dbt Analytics Engineer

## PTTEP Numbers (Real Production)

- 400,000+ sensor records/day
- 1,200+ automated alerts
- 99.5% SLA
- Offshore oil rig platforms
- Predictive maintenance preventing equipment failure
