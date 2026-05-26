# PETROVA — Master Interview Prompt

**Purpose:** Quick-reference cheat sheet for $300K+ Senior Data Engineer interviews. Read this 30 minutes before any interview.

---

## Identity

**Name:** Jay Pechnarai
**Target:** Senior Data Engineer ($300K+)
**Project:** PETROVA — Hybrid Cloud Data Platform for Industrial IoT
**Production Anchor:** PTTEP Asset Performance Management (offshore oil rigs)
**Certifications (in progress):** Databricks DE Associate, SnowPro Core (COF-C03), dbt Analytics Engineer

---

## 30-Second Elevator Pitch

"I built PETROVA, a hybrid cloud data platform that bridges on-premises SAP/SSIS systems with modern cloud — Snowflake, Databricks, and Azure Data Factory. It features a Medallion Architecture with 3-gate validation across 10 silver cleaning logics, an SPC-based statistical alert engine, SCD Type 2 tracking, and traffic-light priority routing — processing 24 pipeline steps across 10 architecture layers with enterprise-grade security."

---

## Numbers That Matter

| Metric | Value |
|--------|-------|
| Architecture layers | 10 |
| Pipeline steps | 24 |
| Silver cleaning logics | 10 across 3 gates |
| SPC alert categories | 6 from 3 statistical primitives |
| RBAC hierarchy | 4 roles (ADMIN → ENGINEER → ANALYST → READER) |
| Gold Immutability Commandments | 7 |
| Recovery window | 97 days (90d Time Travel + 7d Fail-Safe) |
| Python libraries | 11 in production ecosystem |
| dbt tests | 51/51 PASS |
| Technologies | 25+ total, 8 core |

**PTTEP Production Numbers:**
400,000+ sensor records/day, 1,200+ automated alerts, 99.5% SLA, offshore oil rig platforms, predictive maintenance preventing equipment failure.

---

## Architecture Overview (10 Layers)

```
Layer 1:  Source Systems      — SAP BODS, SSIS, IoT sensors, flat files
Layer 2:  Ingestion           — ADF pipelines, Auto Loader, PySpark notebooks
Layer 3:  Bronze (Raw)        — Delta Lake / Snowflake raw tables
Layer 4:  Silver (Clean)      — 3-Gate Validation, 10 cleaning logics, dbt models
Layer 5:  Gold (Business)     — Star schema, SPC alerts, SCD2 dimensions
Layer 6:  Orchestration       — Airflow + ADF + SSIS (triple orchestration)
Layer 7:  Consumption         — Power BI, Streamlit, Jupyter
Layer 8:  Security            — TLS 1.2+, AES-256, Key Vault, RBAC, Unity Catalog
Layer 9:  Monitoring          — PagerDuty, traffic-light routing, composite severity
Layer 10: DevOps              — GitHub Actions, SQLFluff, dbt test, Docker Compose
```

---

## Core Technical Patterns

### 1. Three-Gate Validation (10 Cleaning Logics)

**Gate 1 — Schema (Bronze → Silver):** null detection, type casting, range validation
**Gate 2 — Business (Silver transforms):** status filtering, deduplication, derived columns, SCD2 tracking
**Gate 3 — Referential (Silver → Gold):** referential integrity, aggregation guards, late-arriving data handling

Every record gets quality_flag: PASS, WARN, or FAIL. Only PASS/WARN reach Gold.

### 2. SPC Alert Engine

Three primitives, six alerts:
- **STDDEV** → stability_level + signal_quality
- **LAG** → volatility_trend + spike_detection
- **THRESHOLD** → outlier_flag + range_violation

Composite severity: UNSTABLE + NOISY (CV > 0.3) = CRITICAL. One unstable indicator = WARNING. Everything else = OK.

"Same math as Shewhart (1924) manufacturing quality control."

### 3. SCD Type 2

dbt incremental model with row_hash comparison. Changed records: valid_to = now, is_current = false, new row inserted. Cross-database scd2_merge macro works on PostgreSQL (dev) and Snowflake (prod).

### 4. Triple Orchestration

Airflow + ADF + SSIS working together. Traffic-light priority:
- **RED** = critical pipeline (data freshness SLA)
- **YELLOW** = quality gates (validation checkpoints)
- **GREEN** = analytics/monitoring (dashboards, reports)

### 5. Gold Immutability (7 Commandments)

MERGE-only writes + 4-role RBAC. No DELETE, no TRUNCATE, no DROP on Gold tables.

### 6. Cross-Database Compatibility

dbt macros: dbt.current_timestamp(), dbt.datediff(), generate_schema_name(). Same 16 models deploy to PostgreSQL (dev) and Snowflake (prod) with zero code changes.

### 7. Incremental Processing

Auto Loader (Databricks) + is_incremental() (dbt). Full refresh fallback for schema changes. "Incremental is 100x faster. Full refresh is the escape hatch."

---

## 8 Controlled Trade-offs (Weakness → Strength)

Use these when an interviewer probes for weaknesses. Own the trade-off, then show the mitigation.

| # | Trade-off | One-Liner Resolution |
|---|-----------|---------------------|
| 1 | Complexity | "One `make run` command spins up 6 Docker services. Zero tribal knowledge." |
| 2 | Alert Fatigue | "Composite severity reduces 6 alert types to 3 action levels (OK/WARNING/CRITICAL)." |
| 3 | Logic Duplication | "Spark ingests, dbt transforms — no business logic overlap. dbt is the single source of truth." |
| 4 | Ghost Success | "Fallback flag + freshness indicator + PagerDuty catches stale data within 5 minutes." |
| 5 | Cost / FinOps | "Auto-suspend warehouse + daily batch = pay only for compute that runs." |
| 6 | Data Latency | "400K rows/day doesn't justify streaming infrastructure cost. Intentional batch design." |
| 7 | Cascade Failure | "Fail-fast is intentional — visible failure over silent data corruption." |
| 8 | Skill Dependency | "That's a hiring criteria, not a system flaw." |

**Master insight:** "A senior engineer doesn't eliminate trade-offs — they make them visible, controlled, and reversible."

---

## SLA Protection Strategy (3-Layer Failover)

**Layer A — Partial Async:** trigger_rule='all_done' — pipeline continues even if upstream fails. Failed records quarantined, clean records proceed.

**Layer B — Cached Gold:** Zero-Copy Clone snapshot before each Gold rebuild. If Gold build fails, BI reads from cache with staleness banner.

**Layer C — Dual-Platform:** Databricks down → Snowflake serves Gold. Snowflake down → Databricks Delta tables serve analytics. Both down → cached snapshot in emergency read-only mode.

---

## Dream11 Comparison (Interview Defense)

When asked "how does this compare to real-world platforms?":

- **Dream11:** 200M users, Kafka + Flink + KSQL + Redshift + Neptune + Druid + Elasticsearch. 15+ technologies. Optimizes for **real-time at consumer scale**.
- **PETROVA:** IoT sensors, ADF + Spark + dbt + Snowflake + Airflow. 8 core technologies. Optimizes for **data quality at industrial IoT scale**.
- **PETROVA advantage:** 3-Gate validation, 51 dbt tests, SPC alerts, cross-database portability. Dream11 shows no quality layer.
- **Streaming defense:** "Streaming was avoided because 400K/day doesn't justify Kafka + Flink cost. If volume grew to 10M+/day, we'd add Kafka between ADF and Bronze — the medallion architecture supports that upgrade without redesigning downstream."

---

## VS Code vs dbt Cloud IDE

**VS Code (daily driver):** Full control, offline, free, dbt Power User extension, multi-project. No built-in scheduler/CI — use Airflow + GitHub Actions. Not tested on cert exam.

**dbt Cloud (cert + CI/CD):** Built-in lineage graph, job scheduler, CI/CD, compiled code tab, defer to production. All exam topics. Free plan = 1 project only, requires internet, limited terminal.

**Interview answer:** "I use both. VS Code for daily dev — full control, Docker, Airflow, everything local. dbt Cloud for certification topics and production job scheduling. Complementary, not competing."

---

## PTTEP Production Anchors

Use these to ground every answer in real-world experience:

- "At PTTEP, I designed the Asset Performance Management platform for offshore oil rigs — 400,000+ sensor records per day, 1,200+ automated alerts, 99.5% SLA."
- "Predictive maintenance isn't theoretical for me. When a compressor on an offshore platform shows volatility trending DEGRADING, you don't wait — you trigger maintenance before it fails."
- "The SPC engine in PETROVA is directly inspired by the real-time monitoring I built at PTTEP. The difference is PETROVA makes it reproducible and certifiable."

---

## Technology Stack (25+)

**Cloud:** Azure Data Factory, ADLS Gen2, Azure Key Vault, Databricks, Snowflake
**Compute:** PySpark, Delta Lake, Auto Loader, Structured Streaming
**Transform:** dbt-core, dbt macros, Jinja2, SQLFluff
**Orchestration:** Apache Airflow, ADF pipelines, SSIS packages
**Analytics:** Power BI, Streamlit, Plotly, Jupyter
**Python Libraries (11):** pyspark, delta-spark, dbt-core, great-expectations, snowflake-connector-python, pandas, pyarrow, apache-airflow, streamlit, plotly, sqlalchemy
**DevOps:** Docker Compose, GitHub Actions, pytest
**Legacy:** SAP BODS, SAP ERP, SSIS

---

## Security Posture

- **Network:** TLS 1.2+
- **Storage:** AES-256 at rest (ADLS, Snowflake, Key Vault)
- **Auth:** Key-pair + Azure AD SSO — no passwords in code
- **Authorization:** 4-role RBAC (ADMIN → ENGINEER → ANALYST → READER)
- **Data Protection:** Dynamic masking on PII for ANALYST/READER roles
- **Governance:** Unity Catalog for Databricks, audit logging to AUDIT schema
- **DevSecOps:** SQLFluff + dbt test + security scan in CI before deploy

---

## Common Interview Q&A Quick Reference

**"Why hybrid instead of pure cloud?"**
Real enterprises have legacy systems. SAP BODS and SSIS aren't going away. Triple orchestration shows I can integrate, not just greenfield.

**"How do you ensure data quality?"**
Three gates, 10 cleaning logics. Every record gets quality_flag. Only PASS/WARN reach Gold.

**"Walk me through the alert engine."**
SPC — 3 primitives, 6 alerts. Composite severity routing. At PTTEP, this prevented equipment failures.

**"How do you handle failures?"**
Severity-based routing. CRITICAL → PagerDuty. WARNING → next-shift. Gold has Zero-Copy Clone for instant rollback. 97-day recovery window.

**"Why Databricks AND Snowflake?"**
Different strengths. Databricks = compute-heavy (streaming, PySpark). Snowflake = analytical warehouse (star schema, Time Travel, data sharing).

**"What about security?"**
Defense in depth: TLS 1.2+, AES-256, Key Vault, key-pair auth, 4-role RBAC, dynamic masking, Unity Catalog, audit logging, SQLFluff + dbt test in CI.

**"What would you do differently?"**
"If I started over, I'd implement data contracts at the ingestion boundary. But that requires organizational maturity — valid new sensor columns would be rejected until the schema registry is updated. For a team of 1-3, the 3-gate approach gives 90% of the benefit at 10% of the governance overhead."

---

## Certification Alignment

| Cert | Key Topics You Built | Status |
|------|---------------------|--------|
| Databricks DE Associate | Delta Lake, Auto Loader, Spark, Clusters, Jobs, Unity Catalog | Code complete |
| SnowPro Core (COF-C03) | Warehouses, Time Travel, Zero-Copy Clone, Streams, Tasks, RBAC | DDLs written |
| dbt Analytics Engineer | Models, Tests, Snapshots, Macros, Incremental, Sources, Lineage | 51 tests pass |

---

## 8 Workflow Pattern Diagrams

Located in `docs/workflow_diagrams/` — reference these when whiteboarding:

| # | Pattern | Key Concept |
|---|---------|-------------|
| 1 | SLA Protection | trigger_rule + cached Gold + dual-platform failover |
| 2 | 3-Gate Validation | 10 cleaning logics + quarantine per gate |
| 3 | SPC Alert Engine | 3 primitives → 6 alerts → composite severity |
| 4 | SCD Type 2 | timestamp + check strategies + cross-DB macro |
| 5 | Quarantine & Recovery | tagged records + reason + gate# + zero data loss |
| 6 | Cross-DB Compatibility | write once, run on PostgreSQL + Snowflake |
| 7 | Incremental Processing | Auto Loader + is_incremental() + full refresh fallback |
| 8 | Local E2E Pipeline | Docker Compose → 6 services → 51/51 PASS |

---

## Interview Answer Blueprint

For any "tell me about a weakness" or "what would you improve" question:

```
1. CONTEXT    → "I built a 400K records/day pipeline across Databricks + Snowflake."
2. CANDOR     → Admit the real weakness (e.g., cascade failure risk)
3. MITIGATION → Show you already solved it with engineering
4. TRADE-OFF  → Acknowledge the cost of your solution
5. PROOF      → "Here's the code: trigger_rule, Zero-Copy Clone, PagerDuty."
```

---

## Final Mindset

- You are not defending a school project. You built a **production-grade platform**.
- Every design choice has a **reason and a trade-off you can articulate**.
- Ground every answer in **PTTEP production experience** (400K records, offshore rigs, predictive maintenance).
- The interviewer wants to see **engineering maturity**, not perfection.
- "A senior engineer doesn't eliminate trade-offs — they make them visible, controlled, and reversible."
