# SnowPro Core (COF-C03) — Hands-On Lab Guide

**Account:** `fvakveg-xt38879.snowflakecomputing.com`
**Login:** `etlpetrova097@gmail.com`
**PETROVA DDLs:** `snowflake/ddl/01-10` + `rbac/` + `procedures/`

---

## Study Plan (2-Week Sprint)

| Day | Lab | Exam Domain | Time |
|-----|-----|------------|------|
| 1 | Lab 1: Warehouses & Compute | Architecture & Compute | 2 hrs |
| 2 | Lab 2: Databases, Schemas & Tables | Data Storage | 2 hrs |
| 3 | Lab 3: Data Loading (COPY INTO, Stages) | Data Loading | 2 hrs |
| 4 | Lab 4: Semi-Structured Data (VARIANT, FLATTEN) | Semi-Structured | 2 hrs |
| 5 | Lab 5: Time Travel & Fail-Safe | Data Protection | 2 hrs |
| 6 | Lab 6: Zero-Copy Clone | Data Protection | 1.5 hrs |
| 7 | Lab 7: Streams & Tasks (CDC) | Data Pipelines | 2 hrs |
| 8 | Lab 8: RBAC & Security | Security | 2 hrs |
| 9 | Lab 9: Data Sharing & Masking | Data Sharing & Security | 2 hrs |
| 10 | Lab 10: Resource Monitors & Cost | Performance & Cost | 1.5 hrs |
| 11-12 | Practice Questions + Weak Areas | All Domains | 3 hrs |
| 13-14 | Mock Exam + Final Review | All Domains | 3 hrs |

---

## Lab 1: Warehouses & Compute

**Exam Domain:** Snowflake Architecture & Key Concepts (20-25%)

### What You'll Learn
- Virtual warehouse sizing (XS to 4XL)
- Multi-cluster warehouses (scaling policy)
- Auto-suspend / auto-resume
- Credit consumption model

### Hands-On Exercises

Open a worksheet in your Snowflake account and run these:

```sql
-- Exercise 1.1: Create your PETROVA warehouses
-- File: snowflake/ddl/01_warehouse.sql (run the full file)

-- Exercise 1.2: Check warehouse status
SHOW WAREHOUSES;

-- Exercise 1.3: Understand credit consumption
-- XS = 1 credit/hour, S = 2, M = 4, L = 8, XL = 16
-- PETROVA_DEV_WH (XS) running 2 hours = 2 credits
-- PETROVA_ETL_WH (M) running 30 min = 2 credits (billed per-second, min 60s)

-- Exercise 1.4: Test auto-suspend
ALTER WAREHOUSE PETROVA_DEV_WH SET AUTO_SUSPEND = 60;
-- Wait 60 seconds of inactivity, then check:
SELECT CURRENT_WAREHOUSE();  -- Should still show PETROVA_DEV_WH
SHOW WAREHOUSES LIKE 'PETROVA%';  -- Check STATE column

-- Exercise 1.5: Resize a warehouse (no data movement!)
ALTER WAREHOUSE PETROVA_DEV_WH SET WAREHOUSE_SIZE = 'SMALL';
-- Exam key: resizing doesn't affect running queries
-- New size applies to NEXT query
ALTER WAREHOUSE PETROVA_DEV_WH SET WAREHOUSE_SIZE = 'X-SMALL';  -- Reset

-- Exercise 1.6: Multi-cluster scaling (Enterprise feature)
ALTER WAREHOUSE PETROVA_PROD_WH SET
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD';  -- vs ECONOMY
-- STANDARD: starts new cluster when queue > 0
-- ECONOMY: starts only when 6+ minutes of queue
```

### Exam Questions to Master

1. **Q:** A query runs for 3 minutes on a Medium warehouse. How many credits?
   **A:** M = 4 credits/hour. 3 min = 4 * (3/60) = 0.2 credits. (Billed per-second after 60s minimum)

2. **Q:** What happens to running queries when you resize a warehouse?
   **A:** Nothing. Running queries finish on old size. New size applies to next query.

3. **Q:** STANDARD vs ECONOMY scaling policy?
   **A:** STANDARD adds clusters immediately when queries queue. ECONOMY waits ~6 minutes. STANDARD = lower latency, higher cost.

4. **Q:** Can you resize a multi-cluster warehouse?
   **A:** Yes. Each cluster gets the new size. Total compute = size x cluster_count.

---

## Lab 2: Databases, Schemas & Tables

**Exam Domain:** Snowflake Architecture & Data Storage (20-25%)

### Hands-On Exercises

```sql
-- Exercise 2.1: Create PETROVA database structure
-- File: snowflake/ddl/02_databases_schemas.sql (run the full file)

-- Exercise 2.2: Create Bronze tables
-- File: snowflake/ddl/03_tables_bronze.sql (run the full file)

-- Exercise 2.3: Verify your objects
SHOW DATABASES LIKE 'PETROVA%';
SHOW SCHEMAS IN DATABASE PETROVA_PROD;
SHOW TABLES IN SCHEMA PETROVA_PROD.BRONZE;

-- Exercise 2.4: Understand micro-partitions
-- Insert sample data to see clustering
INSERT INTO PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET
    (sensor_id, sensor_name, reading_value, reading_timestamp, status)
VALUES
    ('S001', 'Vibration A', 12.5, '2025-01-15 08:00:00', 'ACTIVE'),
    ('S001', 'Vibration A', 13.2, '2025-01-15 09:00:00', 'ACTIVE'),
    ('S002', 'Temp B', 75.3, '2025-01-15 08:30:00', 'ACTIVE'),
    ('S002', 'Temp B', 76.1, '2025-01-15 09:30:00', 'MAINTENANCE'),
    ('S003', 'Pressure C', 4.5, '2025-01-16 10:00:00', 'ACTIVE');

-- Exercise 2.5: Check clustering info
SELECT SYSTEM$CLUSTERING_INFORMATION('PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET');

-- Exercise 2.6: Table types (exam topic!)
-- Permanent (default): Time Travel up to 90 days + 7 day Fail-Safe
-- Transient: Time Travel 0-1 day, NO Fail-Safe
-- Temporary: Session only, NO Fail-Safe, not visible to other sessions

CREATE TRANSIENT TABLE PETROVA_PROD.STAGING.TEMP_LOAD (
    id INT, data VARCHAR
) DATA_RETENTION_TIME_IN_DAYS = 0;

CREATE TEMPORARY TABLE MY_SESSION_SCRATCH (id INT, val VARCHAR);
-- This disappears when you close your session!

-- Exercise 2.7: Information Schema (exam loves this)
SELECT table_name, table_type, row_count, bytes
FROM PETROVA_PROD.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'BRONZE';

-- Exercise 2.8: Account Usage (exam topic: 1-year history)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

### Exam Questions to Master

1. **Q:** Transient table vs Temporary table?
   **A:** Transient: persists across sessions, 0-1 day Time Travel, no Fail-Safe. Temporary: session-scoped, disappears when session ends.

2. **Q:** How long is data in Fail-Safe accessible?
   **A:** 7 days. But ONLY Snowflake support can recover it. Not user-accessible.

3. **Q:** INFORMATION_SCHEMA vs ACCOUNT_USAGE?
   **A:** INFORMATION_SCHEMA: real-time, current database only. ACCOUNT_USAGE: up to 1 year history, all databases, 45-minute latency.

---

## Lab 3: Data Loading

**Exam Domain:** Data Loading & Unloading (10-15%)

### Hands-On Exercises

```sql
-- Exercise 3.1: Create file formats
-- File: snowflake/ddl/07_semi_structured.sql (file format section)

CREATE FILE FORMAT IF NOT EXISTS PETROVA_PROD.BRONZE.CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

CREATE FILE FORMAT IF NOT EXISTS PETROVA_PROD.BRONZE.JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE;

CREATE FILE FORMAT IF NOT EXISTS PETROVA_PROD.BRONZE.PARQUET_FORMAT
    TYPE = PARQUET;

-- Exercise 3.2: Internal Stage (for testing without cloud storage)
CREATE STAGE IF NOT EXISTS PETROVA_PROD.BRONZE.STG_INTERNAL
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

-- List stage contents
LIST @PETROVA_PROD.BRONZE.STG_INTERNAL;

-- Exercise 3.3: PUT a file to internal stage (from SnowSQL CLI)
-- PUT file:///tmp/sensor_data.csv @PETROVA_PROD.BRONZE.STG_INTERNAL;

-- Exercise 3.4: COPY INTO from stage
-- COPY INTO PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET
-- FROM @STG_INTERNAL
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
-- ON_ERROR = 'CONTINUE'   -- or SKIP_FILE, ABORT_STATEMENT
-- PURGE = TRUE;            -- Delete staged files after load

-- Exercise 3.5: Understand COPY INTO options (exam topic)
-- ON_ERROR options:
--   CONTINUE     = skip bad rows, load the rest
--   SKIP_FILE    = skip entire file if any error
--   SKIP_FILE_N  = skip file if N+ errors (e.g., SKIP_FILE_3)
--   ABORT_STATEMENT = stop everything on first error (default)

-- Exercise 3.6: Snowpipe (exam topic)
-- File: snowflake/ddl/06_stages_and_tasks.sql
-- Snowpipe = serverless, event-driven auto-ingest
-- Triggered by cloud event notification (Azure Event Grid, AWS SQS, GCP Pub/Sub)
-- Billed per-file (not per-warehouse-second)

-- Check pipe status
-- SELECT SYSTEM$PIPE_STATUS('PETROVA_PROD.BRONZE.PIPE_SENSOR_INGEST');

-- Exercise 3.7: VALIDATE function (exam topic)
-- After COPY INTO, check what was rejected:
-- SELECT * FROM TABLE(VALIDATE(SENSOR_READINGS_PARQUET, JOB_ID => '_last'));
```

### Exam Questions to Master

1. **Q:** Snowpipe vs COPY INTO?
   **A:** Snowpipe: serverless, auto-ingest, event-driven, per-file billing. COPY INTO: manual/scheduled, uses virtual warehouse, per-second billing.

2. **Q:** What does PURGE = TRUE do in COPY INTO?
   **A:** Deletes successfully loaded files from the stage after load completes.

3. **Q:** Named stage vs Table stage vs User stage?
   **A:** Named: `@my_stage`. Table: `@%my_table`. User: `@~`. Named stages are reusable across tables.

---

## Lab 4: Semi-Structured Data

**Exam Domain:** Semi-Structured Data (10-15%)

### Hands-On Exercises

```sql
-- Exercise 4.1: Create table with VARIANT
-- File: snowflake/ddl/07_semi_structured.sql (run full file)

-- Exercise 4.2: Insert JSON data
INSERT INTO PETROVA_PROD.BRONZE.IOT_EVENTS_RAW (event_id, event_timestamp, raw_payload)
SELECT
    'EVT-001', '2025-06-01 08:00:00',
    PARSE_JSON('{
        "sensor_id": "S001",
        "status": "ACTIVE",
        "readings": [
            {"value": 12.5, "unit": "mm/s", "quality": "PASS"},
            {"value": 13.1, "unit": "mm/s", "quality": "PASS"},
            {"value": 99.9, "unit": "mm/s", "quality": "FAIL"}
        ],
        "metadata": {"location": "Platform-A", "rig": "Offshore-3"}
    }');

-- Exercise 4.3: Dot notation queries
SELECT
    event_id,
    raw_payload:sensor_id::VARCHAR AS sensor_id,
    raw_payload:status::VARCHAR AS status,
    raw_payload:metadata.location::VARCHAR AS location,
    ARRAY_SIZE(raw_payload:readings) AS num_readings
FROM PETROVA_PROD.BRONZE.IOT_EVENTS_RAW;

-- Exercise 4.4: FLATTEN (critical exam topic!)
SELECT
    e.event_id,
    f.index AS reading_index,
    f.value:value::FLOAT AS reading_value,
    f.value:unit::VARCHAR AS unit,
    f.value:quality::VARCHAR AS quality
FROM PETROVA_PROD.BRONZE.IOT_EVENTS_RAW e,
    LATERAL FLATTEN(input => e.raw_payload:readings) f;

-- Exercise 4.5: OBJECT_CONSTRUCT and ARRAY_AGG
SELECT
    sensor_id,
    OBJECT_CONSTRUCT(
        'avg', AVG(reading_value),
        'max', MAX(reading_value),
        'count', COUNT(*)
    ) AS stats_json
FROM PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET
GROUP BY sensor_id;

-- Exercise 4.6: TYPEOF (exam topic)
SELECT
    TYPEOF(PARSE_JSON('{"a":1}')),           -- OBJECT
    TYPEOF(PARSE_JSON('[1,2,3]')),            -- ARRAY
    TYPEOF(PARSE_JSON('42')),                 -- INTEGER
    TYPEOF(PARSE_JSON('"hello"'));             -- VARCHAR
```

### Exam Questions to Master

1. **Q:** What does LATERAL FLATTEN do?
   **A:** Explodes an array or object into rows. Each array element becomes a separate row. LATERAL allows the FLATTEN to reference columns from the parent table.

2. **Q:** How do you access the 3rd element of a JSON array?
   **A:** `raw_payload:readings[2]:value` (0-indexed)

3. **Q:** VARIANT vs OBJECT vs ARRAY?
   **A:** VARIANT: any semi-structured type. OBJECT: key-value pairs (JSON object). ARRAY: ordered list. OBJECT and ARRAY are subtypes of VARIANT.

---

## Lab 5: Time Travel & Fail-Safe

**Exam Domain:** Data Protection (10-15%)

### Hands-On Exercises

```sql
-- Exercise 5.1: Run the full Time Travel DDL
-- File: snowflake/ddl/09_time_travel.sql

-- Exercise 5.2: Create a test table and modify it
CREATE TABLE PETROVA_DEV.BRONZE.TT_TEST (id INT, name VARCHAR, val FLOAT);
INSERT INTO PETROVA_DEV.BRONZE.TT_TEST VALUES (1, 'Alpha', 10.0), (2, 'Beta', 20.0);

-- Remember the current time
SELECT CURRENT_TIMESTAMP();  -- Note this: e.g., 2025-06-15 10:30:00

-- Modify data
UPDATE PETROVA_DEV.BRONZE.TT_TEST SET val = 999.0 WHERE id = 1;
DELETE FROM PETROVA_DEV.BRONZE.TT_TEST WHERE id = 2;

-- Exercise 5.3: Time Travel with AT (TIMESTAMP)
SELECT * FROM PETROVA_DEV.BRONZE.TT_TEST
    AT(TIMESTAMP => '2025-06-15 10:30:00'::TIMESTAMP);
-- Shows original data before UPDATE/DELETE!

-- Exercise 5.4: Time Travel with OFFSET
SELECT * FROM PETROVA_DEV.BRONZE.TT_TEST
    AT(OFFSET => -120);  -- 120 seconds ago

-- Exercise 5.5: Time Travel with BEFORE (STATEMENT)
-- Get query ID from QUERY_HISTORY
SELECT query_id, query_text FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%UPDATE%TT_TEST%' ORDER BY start_time DESC LIMIT 1;
-- Then use:
-- SELECT * FROM PETROVA_DEV.BRONZE.TT_TEST BEFORE(STATEMENT => '<query_id>');

-- Exercise 5.6: Recovery via CTAS
CREATE TABLE PETROVA_DEV.BRONZE.TT_TEST_RECOVERED AS
    SELECT * FROM PETROVA_DEV.BRONZE.TT_TEST AT(OFFSET => -300);

-- Exercise 5.7: UNDROP
DROP TABLE PETROVA_DEV.BRONZE.TT_TEST;
UNDROP TABLE PETROVA_DEV.BRONZE.TT_TEST;
SELECT * FROM PETROVA_DEV.BRONZE.TT_TEST;  -- It's back!

-- Exercise 5.8: Set retention periods
ALTER TABLE PETROVA_DEV.BRONZE.TT_TEST
    SET DATA_RETENTION_TIME_IN_DAYS = 90;  -- Enterprise: max 90
SHOW TABLES LIKE 'TT_TEST' IN SCHEMA PETROVA_DEV.BRONZE;  -- Check retention_time

-- Clean up
DROP TABLE PETROVA_DEV.BRONZE.TT_TEST;
DROP TABLE PETROVA_DEV.BRONZE.TT_TEST_RECOVERED;
```

### Exam Questions to Master

1. **Q:** Enterprise edition max Time Travel?
   **A:** 90 days. Standard edition = 0 or 1 day only.

2. **Q:** Fail-Safe duration?
   **A:** Always 7 days. Non-configurable. Only Snowflake support can access.

3. **Q:** Transient table Time Travel + Fail-Safe?
   **A:** Time Travel: 0 or 1 day. Fail-Safe: 0 days (none).

4. **Q:** Total recovery window for Enterprise permanent table?
   **A:** Up to 97 days (90 Time Travel + 7 Fail-Safe).

---

## Lab 6: Zero-Copy Clone

**Exam Domain:** Data Protection (10-15%)

### Hands-On Exercises

```sql
-- Exercise 6.1: Clone a table (instant, zero storage cost initially)
-- OR REPLACE makes this re-runnable (safe to run multiple times)
CREATE OR REPLACE TABLE PETROVA_DEV.BRONZE.SENSOR_CLONE
    CLONE PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET;

-- Exercise 6.2: Verify clone is independent
SELECT COUNT(*) FROM PETROVA_DEV.BRONZE.SENSOR_CLONE;
-- Modify clone (doesn't affect source!)
INSERT INTO PETROVA_DEV.BRONZE.SENSOR_CLONE
    (sensor_id, sensor_name, reading_value, reading_timestamp, status)
VALUES ('CLONE-001', 'Clone Test', 0.0, CURRENT_TIMESTAMP(), 'TEST');

-- Exercise 6.3: Clone a schema (all objects)
CREATE SCHEMA PETROVA_DEV.GOLD_SNAPSHOT CLONE PETROVA_PROD.GOLD;

-- Exercise 6.4: Clone a database (disaster recovery)
CREATE DATABASE PETROVA_BACKUP CLONE PETROVA_PROD;

-- Exercise 6.5: Clone with Time Travel (point-in-time clone)
-- CREATE TABLE GOLD.KPI_BEFORE_RELEASE
--     CLONE GOLD.FCT_DAILY_SENSOR_KPI
--     AT(TIMESTAMP => '2025-06-01 00:00:00'::TIMESTAMP);

-- Exercise 6.6: Clone for dev/testing (PETROVA's Gold snapshot pattern)
-- This is what we use for SLA protection:
-- Before Gold rebuild, clone Gold as cache
-- If rebuild fails, BI reads from clone
-- CREATE SCHEMA PETROVA_PROD.GOLD_CACHE CLONE PETROVA_PROD.GOLD;

-- Clean up
DROP TABLE IF EXISTS PETROVA_DEV.BRONZE.SENSOR_CLONE;
DROP SCHEMA IF EXISTS PETROVA_DEV.GOLD_SNAPSHOT;
DROP DATABASE IF EXISTS PETROVA_BACKUP;
```

### Exam Questions to Master

1. **Q:** Does a clone consume storage immediately?
   **A:** No. Zero additional storage at creation. Only new/modified data in clone consumes storage (copy-on-write).

2. **Q:** Can you clone a clone?
   **A:** Yes. Clones are independent objects.

3. **Q:** Can you clone across databases?
   **A:** Table/schema clones: yes, within same account. Database clone: creates new database.

---

## Lab 7: Streams & Tasks

**Exam Domain:** Data Pipelines (10-15%)

### Hands-On Exercises

```sql
-- Exercise 7.1: Run the Streams DDL
-- File: snowflake/ddl/08_streams.sql

-- Exercise 7.2: Create a test stream
CREATE TABLE PETROVA_DEV.BRONZE.STREAM_TEST (id INT, name VARCHAR, val FLOAT);
CREATE STREAM PETROVA_DEV.BRONZE.STM_TEST
    ON TABLE PETROVA_DEV.BRONZE.STREAM_TEST;

-- Exercise 7.3: Insert data and check stream
INSERT INTO PETROVA_DEV.BRONZE.STREAM_TEST VALUES (1, 'First', 10.0);
SELECT * FROM PETROVA_DEV.BRONZE.STM_TEST;
-- Shows: METADATA$ACTION = INSERT, METADATA$ISUPDATE = FALSE

-- Exercise 7.4: Update and check stream
UPDATE PETROVA_DEV.BRONZE.STREAM_TEST SET val = 99.0 WHERE id = 1;
SELECT * FROM PETROVA_DEV.BRONZE.STM_TEST;
-- Shows 2 rows: DELETE (old) + INSERT (new), both METADATA$ISUPDATE = TRUE

-- Exercise 7.5: Consume stream (resets offset!)
CREATE TABLE PETROVA_DEV.SILVER.STREAM_TARGET (id INT, name VARCHAR, val FLOAT);
INSERT INTO PETROVA_DEV.SILVER.STREAM_TARGET
    SELECT id, name, val FROM PETROVA_DEV.BRONZE.STM_TEST
    WHERE METADATA$ACTION = 'INSERT';
-- Stream is now empty (consumed)
SELECT * FROM PETROVA_DEV.BRONZE.STM_TEST;  -- Empty!

-- Exercise 7.6: Check stream has data
SELECT SYSTEM$STREAM_HAS_DATA('PETROVA_DEV.BRONZE.STM_TEST');  -- FALSE

-- Exercise 7.7: Append-only stream
CREATE STREAM PETROVA_DEV.BRONZE.STM_TEST_APPEND
    ON TABLE PETROVA_DEV.BRONZE.STREAM_TEST
    APPEND_ONLY = TRUE;
-- Only tracks INSERTs (more efficient for append-heavy tables like sensor data)

-- Exercise 7.8: Create a Task
CREATE TASK PETROVA_DEV.BRONZE.TASK_TEST
    WAREHOUSE = PETROVA_DEV_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('PETROVA_DEV.BRONZE.STM_TEST')
AS
    INSERT INTO PETROVA_DEV.SILVER.STREAM_TARGET
    SELECT id, name, val FROM PETROVA_DEV.BRONZE.STM_TEST
    WHERE METADATA$ACTION = 'INSERT';

-- IMPORTANT: Tasks start SUSPENDED! Must resume:
ALTER TASK PETROVA_DEV.BRONZE.TASK_TEST RESUME;

-- Check task status
SHOW TASKS IN SCHEMA PETROVA_DEV.BRONZE;

-- Clean up (suspend before drop!)
ALTER TASK PETROVA_DEV.BRONZE.TASK_TEST SUSPEND;
DROP TASK PETROVA_DEV.BRONZE.TASK_TEST;
DROP STREAM PETROVA_DEV.BRONZE.STM_TEST;
DROP STREAM PETROVA_DEV.BRONZE.STM_TEST_APPEND;
DROP TABLE PETROVA_DEV.SILVER.STREAM_TARGET;
DROP TABLE PETROVA_DEV.BRONZE.STREAM_TEST;
```

### Exam Questions to Master

1. **Q:** Standard vs Append-only stream?
   **A:** Standard: tracks INSERT, UPDATE, DELETE. Append-only: INSERT only (more efficient, common for sensor/log data).

2. **Q:** What happens when you consume a stream?
   **A:** The stream offset advances. Data is no longer visible in the stream. This happens within a DML transaction.

3. **Q:** Tasks are created in what state?
   **A:** SUSPENDED. Must `ALTER TASK ... RESUME` to activate.

4. **Q:** Stream metadata columns?
   **A:** `METADATA$ACTION` (INSERT/DELETE), `METADATA$ISUPDATE` (TRUE/FALSE), `METADATA$ROW_ID`.

---

## Lab 8: RBAC & Security

**Exam Domain:** Account & Security (15-20%)

### Hands-On Exercises

```sql
-- Exercise 8.1: Run the RBAC setup
-- File: snowflake/rbac/roles_and_grants.sql (run the full file)

-- Exercise 8.2: Verify role hierarchy
SHOW GRANTS TO ROLE PETROVA_ANALYST;
SHOW GRANTS TO ROLE PETROVA_READER;
SHOW GRANTS OF ROLE PETROVA_ENGINEER;  -- Who has this role?

-- Exercise 8.3: Test role-based access
USE ROLE PETROVA_READER;
SELECT * FROM PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI LIMIT 5;  -- Should work
-- SELECT * FROM PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET;  -- Should FAIL

USE ROLE PETROVA_ANALYST;
SELECT * FROM PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET LIMIT 5;  -- Should work

-- Reset to admin
USE ROLE ACCOUNTADMIN;

-- Exercise 8.4: Future grants (exam topic)
-- Already in RBAC file: new Gold tables auto-get READER access
SHOW FUTURE GRANTS IN SCHEMA PETROVA_PROD.GOLD;
```

### Exam Questions to Master

1. **Q:** System-defined roles hierarchy?
   **A:** ORGADMIN > ACCOUNTADMIN > SECURITYADMIN + SYSADMIN > USERADMIN > PUBLIC

2. **Q:** Who can create databases?
   **A:** SYSADMIN (and above). Not SECURITYADMIN (manages users/roles only).

3. **Q:** What do FUTURE GRANTS do?
   **A:** Automatically apply grants to objects created in the future. Avoids granting manually each time.

---

## Lab 9: Data Sharing & Masking

**Exam Domain:** Data Sharing (5-10%) & Security (15-20%)

### Hands-On Exercises

```sql
-- Exercise 9.1: Review the sharing DDL
-- File: snowflake/ddl/10_data_sharing_security.sql

-- Exercise 9.2: Dynamic Masking Policy
CREATE OR REPLACE MASKING POLICY PETROVA_PROD.GOLD.MASK_EMAIL AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('PETROVA_ADMIN', 'PETROVA_ENGINEER') THEN val
        WHEN CURRENT_ROLE() = 'PETROVA_ANALYST' THEN REGEXP_REPLACE(val, '.+@', '****@')
        ELSE '**REDACTED**'
    END;

-- Exercise 9.3: Network Policy (know the syntax)
-- CREATE NETWORK POLICY = IP allowlist/blocklist
-- Applied at ACCOUNT level or USER level

-- Exercise 9.4: Resource Monitor
-- File: snowflake/ddl/10_data_sharing_security.sql (resource monitor section)
-- Run the PETROVA_MONTHLY_MONITOR creation
```

---

## Lab 10: Performance & Cost

**Exam Domain:** Performance & Tuning (10-15%)

### Hands-On Exercises

```sql
-- Exercise 10.1: Query Profile (use Snowflake UI)
-- Run a query, then click "Query Profile" in the History tab
-- Look for: remote spilling, partition pruning, join explosion

-- Exercise 10.2: Clustering
SELECT SYSTEM$CLUSTERING_INFORMATION('PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET');
-- Average depth: lower = better. 1.0 = perfect clustering.

-- Exercise 10.3: Caching layers (exam topic!)
-- Layer 1: Result cache (24 hrs, free, exact same query)
-- Layer 2: Local disk cache (SSD on warehouse nodes)
-- Layer 3: Remote disk (cloud storage)

-- Test result cache:
SELECT COUNT(*) FROM PETROVA_PROD.BRONZE.SENSOR_READINGS_PARQUET;
-- Run again immediately — should be instant (result cache hit)

-- Exercise 10.4: Query acceleration (exam topic)
ALTER WAREHOUSE PETROVA_PROD_WH SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
-- Offloads portions of large scans to shared compute pool
```

### Exam Questions to Master

1. **Q:** Three caching layers?
   **A:** Result cache (24h, free), local disk cache (warehouse SSD), remote storage.

2. **Q:** When is result cache invalidated?
   **A:** When underlying data changes, after 24 hours, or when query includes non-deterministic functions.

3. **Q:** Clustering key vs Partitioning?
   **A:** Snowflake has micro-partitions (automatic). Clustering keys define the sort order WITHIN micro-partitions for better pruning.

---

## Cheat Sheet — SnowPro Core Quick Reference

### Architecture
- **3 layers:** Storage (S3/Azure/GCP), Compute (virtual warehouses), Services (query optimizer, metadata, security)
- **Editions:** Standard < Enterprise < Business Critical < VPS
- **Enterprise adds:** 90-day Time Travel, multi-cluster warehouses, materialized views, data masking, row access policies
- **Compute is separate from storage** — scale independently

### Key Limits
- Time Travel: Standard = 1 day, Enterprise = 90 days
- Fail-Safe: 7 days (all editions, non-configurable)
- Warehouse sizes: XS (1 credit/hr) → 4XL (128 credits/hr) — each size doubles
- Max clustering keys: 4 columns recommended
- Streams: standard (all DML) vs append-only (INSERT only)

### Critical Syntax

```sql
-- Time Travel
SELECT * FROM t AT(TIMESTAMP => '...');
SELECT * FROM t AT(OFFSET => -3600);
SELECT * FROM t BEFORE(STATEMENT => 'query_id');

-- Clone
CREATE TABLE t2 CLONE t1;
CREATE TABLE t2 CLONE t1 AT(TIMESTAMP => '...');

-- Stream
CREATE STREAM stm ON TABLE t [APPEND_ONLY = TRUE];
SELECT SYSTEM$STREAM_HAS_DATA('stm');
METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID

-- Task
CREATE TASK t WAREHOUSE = wh SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('stm') AS ...;
ALTER TASK t RESUME;  -- Tasks start SUSPENDED!

-- Masking
CREATE MASKING POLICY mp AS (val TYPE) RETURNS TYPE -> CASE ... END;
ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY mp;

-- Share
CREATE SHARE s; GRANT ... TO SHARE s; ALTER SHARE s ADD ACCOUNTS = ...;
```

### Exam Domain Weights (COF-C03)
| Domain | Weight | Your PETROVA Coverage |
|--------|--------|----------------------|
| Snowflake AI Data Cloud Features & Architecture | 20-25% | DDL 01-02, Architecture |
| Account Access & Security | 20-25% | RBAC, DDL 10 |
| Performance Concepts | 10-15% | DDL 01 (warehouses), clustering |
| Data Loading & Unloading | 10-15% | DDL 06-07 |
| Data Transformations | 20-25% | DDL 04-05, procedures |
| Data Protection & Data Sharing | 10-15% | DDL 09-10 |
