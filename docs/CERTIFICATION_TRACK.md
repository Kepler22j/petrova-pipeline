# 🎓 PETROVA — Certification Track (Snowflake · Databricks · dbt)
_Your project already demonstrates what each exam tests. The cert just validates it._

## Target certs
| Cert | Level | Status | What in PETROVA proves it |
|---|---|---|---|
| **SnowPro Core** | foundational | in progress | warehouses, micro-partitions, **Snowpipe**, RBAC, Time Travel, **Zero-Copy Clone** |
| **Databricks Data Engineer Associate** | associate | in progress | Delta Lake, Auto Loader, PySpark, **DLT**, Unity Catalog, Workflows |
| **dbt Analytics Engineer** | associate | in progress | models (staging→marts), tests, **snapshots/SCD2**, docs/lineage |

---

## SnowPro Core — domain map (you've built these)
| Exam domain | Your PETROVA evidence |
|---|---|
| Architecture (WH, micro-partitions, caching) | `PETROVA_DEV_WH`, multi-cluster, result cache |
| **Data loading (Snowpipe, COPY, stages)** | continuous Snowpipe ingest (see below) |
| Data protection (Time Travel, Fail-safe, **Zero-Copy Clone**) | SLA protection (clone serves last-good) |
| Security / RBAC | 4-role model (ADMIN/ENGINEER/ANALYST/READER) |
| Performance (clustering, scaling, auto-suspend) | Layer 9 query performance |

### ❄️ Snowpipe in your pipeline (continuous ingest — non-CLI/SQL in Snowsight)
```sql
-- 1) Stage points to cloud storage (Azure/S3/GCS)
CREATE STAGE petrova_raw_stage
  URL = 'azure://petrovaraw/sensors/'
  STORAGE_INTEGRATION = petrova_int;

-- 2) Pipe with AUTO_INGEST = new files trigger load via event notification
CREATE PIPE petrova_sensor_pipe AUTO_INGEST = TRUE AS
  COPY INTO bronze_sensor
  FROM @petrova_raw_stage
  FILE_FORMAT = (TYPE = PARQUET);

-- 3) Verify
SELECT SYSTEM$PIPE_STATUS('petrova_sensor_pipe');   -- EXPECT: "executionState":"RUNNING"
```
*Snowpipe = serverless, continuous, pay-per-file → ideal for your dynamic sensor lane (vs ADF batch for static).*

---

## Databricks Data Engineer Associate — domain map
| Exam domain | Your evidence |
|---|---|
| Delta Lake (ACID, time travel, OPTIMIZE) | Bronze/Silver Delta tables |
| Incremental ingest (**Auto Loader** `cloudFiles`) | streaming Bronze |
| ETL with PySpark / Spark SQL | Silver transforms |
| **DLT** (declarative pipelines + expectations) | medallion + 3-gate as `expectations` |
| Unity Catalog (governance/lineage) | catalog + lineage |
| Workflows (jobs, scheduling) | weekly batch |

## dbt Analytics Engineer — domain map
| Exam domain | Your evidence |
|---|---|
| Models (staging → intermediate → marts) | 16 dbt models |
| Tests (not_null, unique, relationships) | 3-gate validation |
| **Snapshots (SCD2)** | `scd2_vendors` |
| Sources, refs, docs, lineage | dbt docs/lineage |

---

## 📅 Study plan (you have the skill — this is exam polish)
| Week | Cert | Focus | Output |
|---|---|---|---|
| 1–2 | **SnowPro Core** | loading (Snowpipe), protection, RBAC, perf | practice exam ≥ 80% → book exam |
| 3–4 | **Databricks DE Associate** | Delta, Auto Loader, DLT, Workflows | practice exam ≥ 80% → book exam |
| 5 | **dbt Analytics Engineer** | tests, snapshots, lineage | book exam |

**Tip:** use **practice exams** (the real differentiator) + your PETROVA project as the hands-on reference. You've done the work — this is recall + exam format.

## 🎤 Interview line
> *"My PETROVA project is my cert prep made real — I've hands-on built Snowpipe ingestion, Zero-Copy Clone, Delta/DLT, Unity Catalog, and dbt snapshots, which are exactly the SnowPro Core, Databricks DE, and dbt exam domains. The certs validate skills I already use."*
