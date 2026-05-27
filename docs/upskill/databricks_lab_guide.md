# Databricks Data Engineer Associate — Hands-On Lab Guide

**Workspace:** `adb-7405607079686031.11.azuredatabricks.net` (East Asia, Premium)
**PETROVA Notebooks:** `databricks/notebooks/00-03` + `dlt/` + `unity_catalog/`

---

## Study Plan (2-Week Sprint)

| Day | Lab | Exam Domain | Time |
|-----|-----|------------|------|
| 1 | Lab 1: Workspace, Clusters & Notebooks | Databricks Platform | 2 hrs |
| 2 | Lab 2: Spark DataFrame API | ELT with Spark | 2.5 hrs |
| 3 | Lab 3: Delta Lake Fundamentals | Delta Lake | 2 hrs |
| 4 | Lab 4: Auto Loader & COPY INTO | Incremental Processing | 2 hrs |
| 5 | Lab 5: Structured Streaming | Incremental Processing | 2 hrs |
| 6 | Lab 6: OPTIMIZE, VACUUM, Z-ORDER | Delta Lake Maintenance | 2 hrs |
| 7 | Lab 7: Delta Live Tables (DLT) | Production Pipelines | 2 hrs |
| 8 | Lab 8: Unity Catalog | Data Governance | 2 hrs |
| 9 | Lab 9: Jobs & Workflows | Production Orchestration | 1.5 hrs |
| 10 | Lab 10: Complex Types & Higher-Order Functions | Advanced Spark | 2 hrs |
| 11-12 | Practice Questions + Weak Areas | All Domains | 3 hrs |
| 13-14 | Mock Exam + Final Review | All Domains | 3 hrs |

---

## Lab 1: Workspace, Clusters & Notebooks

**Exam Domain:** Databricks Lakehouse Platform (24%)

### Hands-On Exercises

1. **Log into your workspace** at `adb-7405607079686031.11.azuredatabricks.net`

2. **Create a cluster:**
   - Name: `petrova-dev`
   - Runtime: DBR 14.x or latest LTS
   - Node type: Standard_DS3_v2 (cheapest)
   - Workers: 0 (single-node for learning)
   - Auto-termination: 30 minutes
   - **Exam key:** Single-node = driver only. Multi-node = driver + workers.

3. **Import PETROVA notebooks:**
   - Upload `databricks/notebooks/01_bronze_ingest.py`
   - Upload `databricks/notebooks/02_structured_streaming.py`
   - Upload `databricks/notebooks/03_delta_optimization.py`

4. **Notebook fundamentals:**
   ```python
   # Cell 1: Check Spark session
   spark.version  # Should show 3.5.x

   # Cell 2: Magic commands (exam topic)
   # %python (default), %sql, %scala, %r, %md, %sh, %run, %fs

   # Cell 3: %run another notebook (exam topic)
   # %run ./01_bronze_ingest  -- Executes in SAME context (shares variables)

   # Cell 4: dbutils (exam topic)
   dbutils.fs.ls("/")  # List DBFS
   dbutils.notebook.run("./01_bronze_ingest", timeout_seconds=300)
   # ^^^ Runs in NEW context (isolated). Returns string result.
   ```

### Exam Questions to Master

1. **Q:** `%run` vs `dbutils.notebook.run()`?
   **A:** `%run`: same execution context, shares variables. `dbutils.notebook.run()`: new context, isolated, returns string.

2. **Q:** What is DBFS?
   **A:** Databricks File System. Abstraction over cloud storage. `/mnt/` = mounted cloud storage. `/FileStore/` = user uploads. `/tmp/` = temporary.

3. **Q:** Cluster modes?
   **A:** Standard (multi-node), Single-node (driver only), High-concurrency (shared, multiple users). Single-node can't run distributed operations.

4. **Q:** What is a Databricks Runtime?
   **A:** Pre-built package with Apache Spark + Delta Lake + libraries. DBR includes ML, Photon (C++ engine), GPU variants.

---

## Lab 2: Spark DataFrame API

**Exam Domain:** ELT with Apache Spark (29%)

### Hands-On Exercises

Open `01_bronze_ingest.py` notebook and run Section 3 (DataFrame API). Then try these:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Exercise 2.1: Read and explore
df = spark.table("petrova_prod.bronze.sensor_readings")
df.printSchema()
df.describe().show()
df.count()

# Exercise 2.2: Column operations (exam tests heavily)
df2 = (df
    .withColumn("reading_date", F.to_date("reading_timestamp"))
    .withColumn("reading_hour", F.hour("reading_timestamp"))
    .withColumn("is_valid", F.col("reading_value").isNotNull() &
                F.col("reading_value").between(-999, 9999))
    .withColumn("quality_flag",
        F.when(F.col("reading_value").isNull(), "FAIL")
         .when(~F.col("reading_value").between(-999, 9999), "WARN")
         .otherwise("PASS"))
)

# Exercise 2.3: Filter, Select, Rename
df3 = (df2
    .filter(F.col("status").isin("ACTIVE", "MAINTENANCE"))
    .select("sensor_id", "reading_value", "quality_flag", "reading_date")
    .withColumnRenamed("sensor_id", "device_id")
)

# Exercise 2.4: Aggregations
agg_df = (df2
    .groupBy("sensor_id", "reading_date")
    .agg(
        F.avg("reading_value").alias("avg_reading"),
        F.max("reading_value").alias("max_reading"),
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("quality_flag") == "PASS", 1).otherwise(0)).alias("pass_count")
    )
)

# Exercise 2.5: Window Functions (exam topic!)
window_spec = Window.partitionBy("sensor_id").orderBy(F.desc("reading_timestamp"))

df_windowed = (df
    .withColumn("row_num", F.row_number().over(window_spec))
    .withColumn("rank", F.rank().over(window_spec))
    .withColumn("dense_rank", F.dense_rank().over(window_spec))
    .withColumn("prev_reading", F.lag("reading_value", 1).over(window_spec))
    .withColumn("next_reading", F.lead("reading_value", 1).over(window_spec))
    .withColumn("running_avg", F.avg("reading_value").over(
        Window.partitionBy("sensor_id").orderBy("reading_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
)

# Exercise 2.6: Deduplication (exam topic)
# Method 1: dropDuplicates
df_dedup1 = df.dropDuplicates(["sensor_id", "reading_timestamp"])

# Method 2: ROW_NUMBER (keep latest)
df_dedup2 = (df
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("sensor_id", "reading_timestamp")
        .orderBy(F.desc("_loaded_at"))))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Exercise 2.7: Joins
df_equipment = spark.table("petrova_prod.bronze.equipment_master")
df_joined = df.join(df_equipment,
    df.equipment_name == df_equipment.equipment_name,
    "left"  # inner, left, right, full, cross, semi, anti
)

# Exercise 2.8: SQL interop (exam topic)
df.createOrReplaceTempView("sensor_readings_temp")
result = spark.sql("""
    SELECT sensor_id, AVG(reading_value) as avg_val
    FROM sensor_readings_temp
    WHERE status = 'ACTIVE'
    GROUP BY sensor_id
    HAVING AVG(reading_value) > 10
""")
```

### Exam Questions to Master

1. **Q:** `dropDuplicates()` vs `distinct()`?
   **A:** `distinct()`: all columns. `dropDuplicates(["col1"])`: specific columns, keeps first occurrence.

2. **Q:** `createOrReplaceTempView` vs `createOrReplaceGlobalTempView`?
   **A:** Temp view: session-scoped. Global temp view: cluster-scoped (across notebooks), accessed via `global_temp.view_name`.

3. **Q:** How to convert DataFrame to SQL table?
   **A:** `df.write.saveAsTable("db.table")` (permanent) or `df.createOrReplaceTempView("name")` (temporary).

4. **Q:** Join types available?
   **A:** inner, left, right, full, cross, semi (left rows that match), anti (left rows that DON'T match).

---

## Lab 3: Delta Lake Fundamentals

**Exam Domain:** Delta Lake (20%)

### Hands-On Exercises

```python
from delta.tables import DeltaTable

# Exercise 3.1: Create a Delta table
df.write.format("delta").mode("overwrite").saveAsTable("petrova_dev.bronze.sensor_test")

# Exercise 3.2: DESCRIBE HISTORY (exam topic)
spark.sql("DESCRIBE HISTORY petrova_dev.bronze.sensor_test").show(truncate=False)
# Every operation logged: version, timestamp, operation, operationMetrics

# Exercise 3.3: Time Travel (exam topic)
# By version
spark.sql("SELECT * FROM petrova_dev.bronze.sensor_test VERSION AS OF 0").show()
# By timestamp
# spark.sql("SELECT * FROM petrova_dev.bronze.sensor_test TIMESTAMP AS OF '2025-06-01'")

# Exercise 3.4: RESTORE (exam topic)
# spark.sql("RESTORE TABLE petrova_dev.bronze.sensor_test TO VERSION AS OF 0")

# Exercise 3.5: MERGE (exam topic — PETROVA's core pattern)
spark.sql("""
    MERGE INTO petrova_dev.silver.sensor_clean AS target
    USING petrova_dev.bronze.sensor_test AS source
    ON target.sensor_id = source.sensor_id
       AND target.reading_timestamp = source.reading_timestamp
    WHEN MATCHED AND source.reading_value != target.reading_value
        THEN UPDATE SET target.reading_value = source.reading_value
    WHEN NOT MATCHED
        THEN INSERT *
""")

# Exercise 3.6: Schema Evolution (exam topic)
# Add new column on write
(df.withColumn("new_col", F.lit("test"))
   .write.format("delta")
   .mode("append")
   .option("mergeSchema", "true")  # Allow schema evolution
   .saveAsTable("petrova_dev.bronze.sensor_test"))

# Exercise 3.7: Delta constraints (exam topic)
spark.sql("""
    ALTER TABLE petrova_dev.bronze.sensor_test
    ADD CONSTRAINT valid_reading CHECK (reading_value IS NOT NULL)
""")
# Constraint violations cause INSERT/UPDATE to fail

# Exercise 3.8: Delta properties
spark.sql("""
    ALTER TABLE petrova_dev.bronze.sensor_test SET TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
```

### Exam Questions to Master

1. **Q:** What is the Delta transaction log?
   **A:** `_delta_log/` directory with JSON files for each commit. Provides ACID transactions, Time Travel, and audit trail. Checkpoint every 10 commits (Parquet).

2. **Q:** MERGE operation atomicity?
   **A:** MERGE is a single atomic transaction. All matched/not-matched actions succeed or all fail.

3. **Q:** Schema enforcement vs schema evolution?
   **A:** Enforcement (default): rejects writes with wrong schema. Evolution (`mergeSchema=true`): adds new columns automatically.

4. **Q:** Delta vs Parquet?
   **A:** Delta = Parquet + transaction log + ACID + Time Travel + MERGE + schema enforcement.

---

## Lab 4: Auto Loader & COPY INTO

**Exam Domain:** Incremental Data Processing (22%)

### Hands-On Exercises

Open `01_bronze_ingest.py` notebook. Key concepts:

```python
# Exercise 4.1: Auto Loader (cloudFiles) — exam's #1 ingestion topic
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/mnt/petrova/schemas/sensors")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/mnt/petrova/landing/sensors/")
)

# Exercise 4.2: Write stream to Delta
(df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/petrova/checkpoints/bronze")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("petrova_prod.bronze.sensor_readings")
)

# Exercise 4.3: COPY INTO (batch alternative)
spark.sql("""
    COPY INTO petrova_prod.bronze.sap_orders
    FROM '/mnt/petrova/landing/sap/orders/'
    FILEFORMAT = PARQUET
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
# COPY INTO is idempotent: tracks loaded files, skips already-loaded ones
```

### Exam Questions to Master

1. **Q:** Auto Loader vs COPY INTO?
   **A:** Auto Loader: streaming, scales to millions of files, file notification mode (more efficient). COPY INTO: batch, directory listing mode, simpler setup.

2. **Q:** Auto Loader trigger modes?
   **A:** `availableNow=True`: process all files, then stop. `processingTime="1 hour"`: micro-batch every hour. `once=True` (deprecated, use availableNow).

3. **Q:** What does schemaLocation do?
   **A:** Stores inferred schema for Auto Loader. On restart, uses cached schema instead of re-inferring. Required for schema evolution tracking.

4. **Q:** Auto Loader file discovery modes?
   **A:** Directory listing (default, polls directory) vs File notification (event-driven, more efficient for large directories).

---

## Lab 5: Structured Streaming

**Exam Domain:** Incremental Data Processing (22%)

### Hands-On Exercises

Open `02_structured_streaming.py` notebook. Key concepts:

```python
# Exercise 5.1: Trigger modes (exam topic)
# availableNow=True — process all, stop (replaces trigger(once=True))
# processingTime="30 seconds" — micro-batch every 30s
# continuous="1 second" — experimental low-latency (rarely on exam)
# (no trigger) — default micro-batch, process as fast as possible

# Exercise 5.2: Output modes (exam topic)
# append — only new rows (default, most common)
# complete — entire aggregated result (requires aggregation)
# update — only changed rows (requires aggregation)

# Exercise 5.3: Watermark (exam topic)
from pyspark.sql import functions as F

watermarked = (stream_df
    .withWatermark("reading_timestamp", "2 hours")
    .groupBy(
        "sensor_id",
        F.window("reading_timestamp", "1 hour")
    )
    .agg(F.avg("reading_value").alias("avg_reading"))
)
# Watermark = "drop data arriving more than 2 hours late"
# Window = "aggregate into 1-hour tumbling windows"

# Exercise 5.4: Change Data Feed (CDF)
spark.sql("""
    ALTER TABLE petrova_prod.silver.sensor_readings_cleaned
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
# Read CDF:
cdf = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("petrova_prod.silver.sensor_readings_cleaned")
)
# CDF columns: _change_type (insert/update_preimage/update_postimage/delete),
#              _commit_version, _commit_timestamp

# Exercise 5.5: Checkpoint (exam topic)
# checkpointLocation stores: current offsets, committed batch IDs
# Enables exactly-once processing on restart
# NEVER delete checkpoint if you want to resume from where you stopped
```

### Exam Questions to Master

1. **Q:** What happens if you delete the checkpoint?
   **A:** Stream restarts from the beginning. All data reprocessed. May cause duplicates in the sink.

2. **Q:** Can you use COMPLETE output mode without aggregation?
   **A:** No. COMPLETE requires an aggregation operation.

3. **Q:** CDF `_change_type` values?
   **A:** `insert`, `update_preimage` (old row), `update_postimage` (new row), `delete`.

4. **Q:** Watermark = "2 hours" means?
   **A:** Engine tracks max event time seen. Data with timestamp < (max_event_time - 2_hours) is dropped.

---

## Lab 6: OPTIMIZE, VACUUM, Z-ORDER

**Exam Domain:** Delta Lake (20%)

### Hands-On Exercises

Open `03_delta_optimization.py` notebook and run all cells. Key commands:

```sql
-- Exercise 6.1: OPTIMIZE (compacts small files)
OPTIMIZE petrova_prod.silver.sensor_readings_cleaned;
-- Returns: numFilesAdded, numFilesRemoved

-- Exercise 6.2: Z-ORDER (data skipping)
OPTIMIZE petrova_prod.silver.sensor_readings_cleaned
ZORDER BY (sensor_id, reading_date);
-- Co-locates rows with same sensor_id + date in same files
-- Enables file-level min/max pruning

-- Exercise 6.3: VACUUM (reclaim storage)
VACUUM petrova_prod.silver.sensor_readings_cleaned RETAIN 168 HOURS;
-- DRY RUN first:
VACUUM petrova_prod.silver.sensor_readings_cleaned DRY RUN;

-- Exercise 6.4: DESCRIBE HISTORY / DETAIL
DESCRIBE HISTORY petrova_prod.silver.sensor_readings_cleaned LIMIT 10;
DESCRIBE DETAIL petrova_prod.silver.sensor_readings_cleaned;
```

### Exam Questions to Master

1. **Q:** VACUUM default retention?
   **A:** 168 hours (7 days). To go below, must disable safety check.

2. **Q:** OPTIMIZE is idempotent?
   **A:** Yes. Running it twice in a row: second run is a no-op.

3. **Q:** Does OPTIMIZE delete old files?
   **A:** No! OPTIMIZE creates new compacted files. Old files stay for Time Travel. VACUUM deletes them.

4. **Q:** Z-ORDER vs Partitioning?
   **A:** Partitioning: physical directory separation (`col=value/`). Z-ORDER: data co-location WITHIN files. Use Z-ORDER for high-cardinality columns, partitioning for low-cardinality.

---

## Lab 7: Delta Live Tables (DLT)

**Exam Domain:** Production Pipelines (16%)

### Hands-On Exercises

Review `databricks/dlt/petrova_dlt_pipeline.py`:

```python
import dlt
from pyspark.sql import functions as F

# Exercise 7.1: Streaming table (continuously updated)
@dlt.table(comment="Raw sensor readings from landing zone")
def bronze_sensor_readings():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/mnt/petrova/landing/sensors/"))

# Exercise 7.2: Materialized view (batch, recomputed)
@dlt.table(comment="Cleaned sensor readings")
@dlt.expect_or_drop("valid_reading", "reading_value IS NOT NULL")
@dlt.expect_or_fail("valid_sensor", "sensor_id IS NOT NULL")
def silver_sensor_cleaned():
    return dlt.read_stream("bronze_sensor_readings").select(...)

# Exercise 7.3: Expectations (data quality)
# @dlt.expect("name", "condition")         — warn only, keep record
# @dlt.expect_or_drop("name", "condition") — drop failing records
# @dlt.expect_or_fail("name", "condition") — fail entire pipeline
```

### Exam Questions to Master

1. **Q:** DLT `@dlt.expect` vs `@dlt.expect_or_drop` vs `@dlt.expect_or_fail`?
   **A:** expect: warn only (record passes). expect_or_drop: drop failing record. expect_or_fail: abort pipeline.

2. **Q:** Streaming table vs Materialized view in DLT?
   **A:** Streaming: append-only, incrementally updated. Materialized view: can be recomputed fully each run.

3. **Q:** Can you schedule a DLT pipeline?
   **A:** Yes. Via Databricks Workflows. Can be triggered or scheduled (cron).

---

## Lab 8: Unity Catalog

**Exam Domain:** Data Governance (16%)

### Hands-On Exercises

Review `databricks/unity_catalog/setup.sql`:

```sql
-- Exercise 8.1: Three-level namespace (exam topic)
-- catalog.schema.table
USE CATALOG petrova_prod;
USE SCHEMA bronze;
SELECT * FROM sensor_readings;
-- Equivalent: SELECT * FROM petrova_prod.bronze.sensor_readings;

-- Exercise 8.2: Managed vs External tables (exam topic)
-- Managed: UC controls storage. DROP = deletes data.
-- External: data at LOCATION. DROP = removes metadata only.

-- Exercise 8.3: Grants
GRANT USE CATALOG ON CATALOG petrova_prod TO `data-analysts`;
GRANT SELECT ON SCHEMA petrova_prod.gold TO `data-analysts`;

-- Exercise 8.4: Data Lineage
-- Automatic! Check Catalog Explorer → table → Lineage tab
-- Shows upstream/downstream dependencies and column-level lineage
```

### Exam Questions to Master

1. **Q:** Unity Catalog hierarchy?
   **A:** Metastore → Catalog → Schema → Table/View/Function

2. **Q:** DROP managed table vs DROP external table?
   **A:** Managed: deletes data AND metadata. External: deletes metadata only, data stays.

3. **Q:** Where is lineage tracked?
   **A:** Automatically by Unity Catalog. Visible in Catalog Explorer UI.

---

## Lab 9: Jobs & Workflows

**Exam Domain:** Production Pipelines (16%)

### Hands-On Exercises

1. **Create a Job in Databricks UI:**
   - Workflows → Create Job
   - Task 1: Run `01_bronze_ingest` notebook
   - Task 2: Run `03_delta_optimization` (depends on Task 1)
   - Cluster: `petrova-dev`
   - Schedule: Daily at 03:00

2. **Key settings to know:**
   - Retry policy: max retries, min retry interval
   - Timeout: max run duration
   - Alerts: email on failure
   - Concurrent runs: allow or skip

### Exam Questions to Master

1. **Q:** Multi-task job dependency?
   **A:** Tasks can depend on other tasks. Use "Depends on" to create DAG-like dependencies.

2. **Q:** Job cluster vs All-purpose cluster?
   **A:** Job cluster: created for job, terminated after. Cheaper (automated pricing). All-purpose: interactive, stays running.

---

## Lab 10: Complex Types & Higher-Order Functions

**Exam Domain:** ELT with Spark (29%)

### Hands-On Exercises

From `01_bronze_ingest.py` Sections 4-5:

```sql
-- Exercise 10.1: ARRAY functions
SELECT
    sensor_id,
    COLLECT_LIST(reading_value) AS all_readings,
    ARRAY_DISTINCT(COLLECT_LIST(status)) AS unique_statuses,
    SIZE(COLLECT_LIST(reading_value)) AS count
FROM bronze.sensor_readings
GROUP BY sensor_id;

-- Exercise 10.2: EXPLODE (reverse of COLLECT)
SELECT sensor_id, exploded_reading
FROM (SELECT sensor_id, COLLECT_LIST(reading_value) AS readings
      FROM bronze.sensor_readings GROUP BY sensor_id),
LATERAL VIEW EXPLODE(readings) AS exploded_reading;

-- Exercise 10.3: Higher-Order Functions (exam topic!)
-- TRANSFORM: apply function to each element
SELECT TRANSFORM(ARRAY(1,2,3), x -> x * 2);  -- [2,4,6]

-- FILTER: keep elements matching condition
SELECT FILTER(ARRAY(1,2,3,4,5), x -> x > 3);  -- [4,5]

-- REDUCE: fold array to single value
SELECT REDUCE(ARRAY(1,2,3,4), 0, (acc, x) -> acc + x);  -- 10

-- EXISTS: check if any element matches
SELECT EXISTS(ARRAY(1,2,3), x -> x > 2);  -- true
```

### Exam Questions to Master

1. **Q:** EXPLODE vs POSEXPLODE?
   **A:** EXPLODE: one row per element. POSEXPLODE: one row per element + position index.

2. **Q:** Higher-order function syntax?
   **A:** `FUNCTION(array, lambda)`. Lambda: `x -> expression` or `(acc, x) -> expression` for REDUCE.

---

## Cheat Sheet — Databricks DE Associate Quick Reference

### Exam Domain Weights
| Domain | Weight | Your PETROVA Coverage |
|--------|--------|----------------------|
| Databricks Lakehouse Platform | 24% | Workspace, DBFS, clusters |
| ELT with Apache Spark | 29% | DataFrame API, SQL, joins, windows |
| Incremental Data Processing | 22% | Auto Loader, Streaming, CDF |
| Production Pipelines | 16% | DLT, Jobs, Workflows |
| Data Governance | 9% | Unity Catalog, grants, lineage |

### Critical Commands

```python
# Read/Write Delta
df = spark.table("catalog.schema.table")
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.table")

# Auto Loader
spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").load(path)

# Streaming write
df.writeStream.format("delta").option("checkpointLocation",cp).trigger(availableNow=True).table(t)

# MERGE
MERGE INTO target USING source ON condition
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...

# Delta maintenance
OPTIMIZE table [ZORDER BY (col1, col2)]
VACUUM table [RETAIN n HOURS]
DESCRIBE HISTORY table
DESCRIBE DETAIL table

# Time Travel
SELECT * FROM table VERSION AS OF n
SELECT * FROM table TIMESTAMP AS OF '...'
RESTORE TABLE table TO VERSION AS OF n
```

### Key Differences to Remember

| Concept | Option A | Option B |
|---------|----------|----------|
| Auto Loader vs COPY INTO | Streaming, scales to millions | Batch, simpler |
| trigger(once) vs availableNow | Deprecated | Preferred (processes all micro-batches) |
| Managed vs External table | DROP deletes data | DROP keeps data |
| %run vs dbutils.notebook.run | Same context | New context |
| OPTIMIZE vs VACUUM | Creates new files | Deletes old files |
| Schema enforcement vs evolution | Rejects bad schema | Adds new columns |
| Append vs Complete output | New rows only | Full result (needs agg) |
| Job cluster vs All-purpose | Auto-created, cheaper | Interactive, persistent |
