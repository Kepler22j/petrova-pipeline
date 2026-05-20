# Databricks notebook source
# MAGIC %md
# MAGIC # PETROVA 300K – Delta Lake Optimization (Databricks DE Associate Exam)
# MAGIC Topics: OPTIMIZE, VACUUM, Z-ORDER, file compaction, time travel cleanup
# MAGIC
# MAGIC **Why this matters:**
# MAGIC Small files kill Spark performance. OPTIMIZE compacts them. VACUUM reclaims
# MAGIC storage from old versions. Z-ORDER co-locates related data for faster reads.
# MAGIC These three commands are the "maintenance trifecta" for any Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. OPTIMIZE — File Compaction (exam topic)
# MAGIC
# MAGIC Delta tables accumulate small files from streaming writes, appends, and merges.
# MAGIC OPTIMIZE rewrites them into ~1 GB files (default target).
# MAGIC
# MAGIC **Exam key points:**
# MAGIC - OPTIMIZE is idempotent (safe to run repeatedly)
# MAGIC - Does NOT delete old files (that's VACUUM's job)
# MAGIC - Can target specific partitions with WHERE clause

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic OPTIMIZE: compact all small files in the table
# MAGIC OPTIMIZE petrova_prod.bronze.sensor_readings;
# MAGIC
# MAGIC -- Partition-targeted OPTIMIZE (cheaper — only touches recent data)
# MAGIC OPTIMIZE petrova_prod.silver.sensor_readings_cleaned
# MAGIC WHERE reading_date >= current_date() - INTERVAL 7 DAYS;
# MAGIC
# MAGIC -- Check metrics after OPTIMIZE
# MAGIC -- Returns: numFilesAdded, numFilesRemoved, metrics.numBatches

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Z-ORDER — Data Skipping (exam topic)
# MAGIC
# MAGIC Z-ORDER physically co-locates rows with similar values in the same files.
# MAGIC This enables data skipping: Spark reads file-level min/max stats and
# MAGIC skips entire files that can't contain matching rows.
# MAGIC
# MAGIC **Exam key points:**
# MAGIC - Z-ORDER works WITH OPTIMIZE (same command)
# MAGIC - Choose columns used in WHERE/JOIN/GROUP BY filters
# MAGIC - Max 4 columns recommended (diminishing returns)
# MAGIC - NOT the same as partitioning (Z-ORDER works within partitions)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE + Z-ORDER: compact AND co-locate by sensor_id + reading_date
# MAGIC -- This makes queries like "WHERE sensor_id = 'S001' AND reading_date = '2025-01-15'" fast
# MAGIC OPTIMIZE petrova_prod.silver.sensor_readings_cleaned
# MAGIC ZORDER BY (sensor_id, reading_date);
# MAGIC
# MAGIC -- Gold layer: Z-ORDER facts by common query patterns
# MAGIC OPTIMIZE petrova_prod.gold.daily_sensor_kpi
# MAGIC ZORDER BY (sensor_id, kpi_date);
# MAGIC
# MAGIC -- Revenue table: Z-ORDER by date + material (common BI filters)
# MAGIC OPTIMIZE petrova_prod.gold.daily_revenue
# MAGIC ZORDER BY (revenue_date, material_group);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VACUUM — Storage Reclamation (exam topic)
# MAGIC
# MAGIC OPTIMIZE creates new files but leaves old ones for Time Travel.
# MAGIC VACUUM deletes files older than the retention period.
# MAGIC
# MAGIC **Exam key points:**
# MAGIC - Default retention: 7 days (168 hours)
# MAGIC - Files newer than retention are NEVER deleted
# MAGIC - Time Travel won't work for versions older than VACUUM threshold
# MAGIC - VACUUM is irreversible — once deleted, data is gone
# MAGIC - Must set spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC   to vacuum with < 7 day retention (exam trick question)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM with default 7-day retention (safe for production)
# MAGIC VACUUM petrova_prod.bronze.sensor_readings;
# MAGIC
# MAGIC -- VACUUM with explicit retention (exam: hours, not days)
# MAGIC VACUUM petrova_prod.silver.sensor_readings_cleaned RETAIN 168 HOURS;
# MAGIC
# MAGIC -- DRY RUN: see what would be deleted WITHOUT actually deleting
# MAGIC VACUUM petrova_prod.gold.daily_sensor_kpi DRY RUN;

# COMMAND ----------

# Dangerous: VACUUM with 0 hours (exam scenario — know the guard rail)
# This WILL fail unless you disable the safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Now 0-hour VACUUM works (deletes ALL old files immediately)
# spark.sql("VACUUM petrova_prod.bronze.sensor_readings RETAIN 0 HOURS")
# ^^^ COMMENTED OUT — never do this in production!

# Reset safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DESCRIBE HISTORY — Audit Trail (exam topic)
# MAGIC
# MAGIC Every Delta operation is logged. DESCRIBE HISTORY shows the full audit trail.
# MAGIC Key for compliance, debugging, and understanding what OPTIMIZE/VACUUM did.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full history (all operations: WRITE, MERGE, OPTIMIZE, VACUUM, etc.)
# MAGIC DESCRIBE HISTORY petrova_prod.silver.sensor_readings_cleaned;
# MAGIC
# MAGIC -- Recent history only
# MAGIC DESCRIBE HISTORY petrova_prod.gold.daily_sensor_kpi LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. DESCRIBE DETAIL — Table Metadata (exam topic)
# MAGIC
# MAGIC Shows physical stats: number of files, total size, partition columns, etc.
# MAGIC Use this BEFORE and AFTER OPTIMIZE to prove it worked.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table stats: numFiles, sizeInBytes, partitionColumns
# MAGIC DESCRIBE DETAIL petrova_prod.silver.sensor_readings_cleaned;
# MAGIC
# MAGIC -- Compare before/after OPTIMIZE:
# MAGIC -- Before: numFiles=1247, sizeInBytes=523MB (lots of small files)
# MAGIC -- After:  numFiles=12,   sizeInBytes=510MB (compacted, ~same total size)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Local Delta Lab — OPTIMIZE + VACUUM Demo
# MAGIC
# MAGIC Since we can't run cloud SQL from local Docker, here's the PySpark equivalent
# MAGIC that runs against our local Delta tables.

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

# Connect to local Delta (same config as 00_local_delta_lab.ipynb)
spark = (SparkSession.builder
    .appName("PETROVA-DeltaOptimization")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.catalogs.DeltaCatalog")
    .getOrCreate()
)

DELTA_BASE = "/tmp/petrova_delta"

# COMMAND ----------

# Check current file count for Bronze sensor table
bronze_path = f"{DELTA_BASE}/bronze/sensor_readings"

if os.path.exists(bronze_path):
    dt = DeltaTable.forPath(spark, bronze_path)

    # Show table detail (equivalent to DESCRIBE DETAIL)
    detail = dt.detail()
    detail.select("numFiles", "sizeInBytes", "partitionColumns", "createdAt").show(truncate=False)

    # Show history (equivalent to DESCRIBE HISTORY)
    history = dt.history()
    history.select("version", "timestamp", "operation", "operationMetrics").show(10, truncate=False)
else:
    print(f"Table not found at {bronze_path}. Run 00_local_delta_lab.ipynb first.")

# COMMAND ----------

# OPTIMIZE equivalent in PySpark (Delta OSS)
# Note: In open-source Delta, OPTIMIZE is available via SQL or DeltaTable API
if os.path.exists(bronze_path):
    dt = DeltaTable.forPath(spark, bronze_path)

    # Compact small files (OSS Delta Lake 3.x supports optimize())
    dt.optimize().executeCompaction()

    print("OPTIMIZE complete — check file count reduction:")
    dt.detail().select("numFiles", "sizeInBytes").show()

# COMMAND ----------

# Z-ORDER equivalent in PySpark
if os.path.exists(bronze_path):
    dt = DeltaTable.forPath(spark, bronze_path)

    # Z-ORDER by sensor_id (most common filter column)
    dt.optimize().executeZOrderBy("sensor_id")

    print("Z-ORDER by sensor_id complete")
    dt.detail().select("numFiles", "sizeInBytes").show()

# COMMAND ----------

# VACUUM in PySpark
if os.path.exists(bronze_path):
    dt = DeltaTable.forPath(spark, bronze_path)

    # Dry run first (returns list of files that WOULD be deleted)
    files_to_delete = dt.vacuum(retentionHours=168)  # 7 days
    print(f"Files eligible for vacuum: {files_to_delete.count()}")

    # Actual vacuum (uncomment to execute)
    # dt.vacuum(retentionHours=168)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Maintenance Schedule — Production Best Practice
# MAGIC
# MAGIC | Command | Frequency | Target | Retention |
# MAGIC |---------|-----------|--------|-----------|
# MAGIC | OPTIMIZE | Daily (off-peak) | All Silver + Gold tables | N/A |
# MAGIC | Z-ORDER | Weekly | High-query tables | N/A |
# MAGIC | VACUUM | Weekly | All tables | 168 hours (7 days) |
# MAGIC | DESCRIBE HISTORY | On-demand | Audit / debugging | N/A |
# MAGIC
# MAGIC **Airflow DAG pattern:**
# MAGIC ```python
# MAGIC # In petrova_maintenance_dag.py
# MAGIC optimize_task = SparkSqlOperator(
# MAGIC     task_id='optimize_silver',
# MAGIC     sql='OPTIMIZE petrova_prod.silver.sensor_readings_cleaned ZORDER BY (sensor_id, reading_date)',
# MAGIC     dag=dag
# MAGIC )
# MAGIC vacuum_task = SparkSqlOperator(
# MAGIC     task_id='vacuum_silver',
# MAGIC     sql='VACUUM petrova_prod.silver.sensor_readings_cleaned RETAIN 168 HOURS',
# MAGIC     dag=dag
# MAGIC )
# MAGIC optimize_task >> vacuum_task
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exam Cheat Sheet — OPTIMIZE / VACUUM / Z-ORDER
# MAGIC
# MAGIC | Concept | Key Fact |
# MAGIC |---------|----------|
# MAGIC | OPTIMIZE | Compacts small files → fewer, larger files |
# MAGIC | OPTIMIZE + WHERE | Target specific partitions (cheaper) |
# MAGIC | Z-ORDER | Co-locates data for data skipping |
# MAGIC | Z-ORDER columns | Pick filter/join columns, max 4 |
# MAGIC | VACUUM | Deletes files older than retention |
# MAGIC | VACUUM default | 168 hours (7 days) |
# MAGIC | VACUUM < 7 days | Requires retentionDurationCheck = false |
# MAGIC | VACUUM + Time Travel | Versions older than VACUUM retention are lost |
# MAGIC | DESCRIBE HISTORY | Shows all operations (audit log) |
# MAGIC | DESCRIBE DETAIL | Shows numFiles, sizeInBytes, partitions |
# MAGIC | DRY RUN | Preview VACUUM without deleting |
# MAGIC | Idempotent? | OPTIMIZE = yes, VACUUM = yes (no-op if nothing to clean) |
