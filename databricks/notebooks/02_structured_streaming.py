# Databricks notebook source
# MAGIC %md
# MAGIC # PETROVA 300K – Structured Streaming (Databricks DE Associate Exam)
# MAGIC Topics: readStream, writeStream, triggers, watermarks, output modes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Basic Stream Read/Write

# COMMAND ----------

# Read stream from Delta table (exam topic: Delta as streaming source)
stream_df = (spark.readStream
    .format("delta")
    .table("petrova_prod.bronze.sensor_readings")
)

# Write stream to Silver with processing
processed = (stream_df
    .withColumn("quality_flag",
        F.when(F.col("reading_value").isNull(), "FAIL")
         .when(F.col("reading_value").between(-999, 9999), "PASS")
         .otherwise("WARN")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Trigger Modes (exam topic)

# COMMAND ----------

from pyspark.sql import functions as F

# Trigger: availableNow (process all available, then stop – replaces trigger once)
query1 = (processed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/petrova/checkpoints/silver_stream1")
    .outputMode("append")
    .trigger(availableNow=True)
    .table("petrova_prod.silver.sensor_stream_batch")
)

# Trigger: processingTime (micro-batch at interval)
query2 = (processed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/petrova/checkpoints/silver_stream2")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .table("petrova_prod.silver.sensor_stream_micro")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Output Modes (exam topic)

# COMMAND ----------

# APPEND: Only new rows (default, most common)
# COMPLETE: Entire result table (requires aggregation)
# UPDATE: Only changed rows (requires aggregation)

# Example: Complete mode with aggregation
agg_stream = (stream_df
    .groupBy("sensor_id", F.window("reading_timestamp", "1 hour"))
    .agg(F.avg("reading_value").alias("avg_reading"))
)

query3 = (agg_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/petrova/checkpoints/silver_agg")
    .outputMode("complete")  # Rewrites entire result each micro-batch
    .trigger(processingTime="1 minute")
    .table("petrova_prod.silver.sensor_hourly_agg")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Watermarks (exam topic: handling late-arriving data)

# COMMAND ----------

# Watermark: allow data up to 2 hours late
watermarked = (stream_df
    .withWatermark("reading_timestamp", "2 hours")  # Late data threshold
    .groupBy(
        "sensor_id",
        F.window("reading_timestamp", "1 hour")     # Tumbling window
    )
    .agg(
        F.avg("reading_value").alias("avg_reading"),
        F.count("*").alias("count")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Change Data Feed (CDF) – exam topic

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDF on a Delta table
# MAGIC ALTER TABLE petrova_prod.silver.sensor_readings_cleaned
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- Read CDF (track INSERT, UPDATE, DELETE operations)
# MAGIC SELECT * FROM table_changes('petrova_prod.silver.sensor_readings_cleaned', 1)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage');
# MAGIC
# MAGIC -- CDF columns: _change_type, _commit_version, _commit_timestamp
