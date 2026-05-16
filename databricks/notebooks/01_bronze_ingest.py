# Databricks notebook source
# MAGIC %md
# MAGIC # PETROVA 300K – Bronze Ingestion (Databricks DE Associate Exam)
# MAGIC Topics: Auto Loader, COPY INTO, Spark DataFrame API, Schema Evolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader (Structured Streaming for file ingestion)
# MAGIC Key exam topic: cloudFiles source, schema inference, rescue column

# COMMAND ----------

# Auto Loader – incrementally ingest new Parquet files
df_sensors = (spark.readStream
    .format("cloudFiles")                           # Auto Loader format
    .option("cloudFiles.format", "parquet")          # Source file format
    .option("cloudFiles.schemaLocation", "/mnt/petrova/schemas/sensors")  # Schema tracking
    .option("cloudFiles.inferColumnTypes", "true")   # Type inference
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle new columns
    .load("/mnt/petrova/landing/sensors/")
)

# Write to Bronze Delta table
(df_sensors.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/petrova/checkpoints/bronze_sensors")
    .option("mergeSchema", "true")                  # Allow schema evolution
    .outputMode("append")
    .trigger(availableNow=True)                     # Process all available files, then stop
    .table("petrova_prod.bronze.sensor_readings")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. COPY INTO (batch ingestion alternative)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COPY INTO – idempotent batch loading (exam topic: vs Auto Loader)
# MAGIC COPY INTO petrova_prod.bronze.sap_orders
# MAGIC FROM '/mnt/petrova/landing/sap/orders/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spark DataFrame API (exam tests these heavily)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze table
df = spark.table("petrova_prod.bronze.sensor_readings")

# Common transformations tested in exam
df_transformed = (df
    # Column operations
    .withColumn("reading_date", F.to_date("reading_timestamp"))
    .withColumn("reading_hour", F.hour("reading_timestamp"))
    .withColumn("is_valid", F.when(
        (F.col("reading_value").isNotNull()) &
        (F.col("reading_value").between(-999, 9999)),
        F.lit(True)
    ).otherwise(F.lit(False)))

    # Filter
    .filter(F.col("status").isin("ACTIVE", "MAINTENANCE"))

    # Window function (exam topic)
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("sensor_id").orderBy(F.desc("reading_timestamp"))
    ))

    # Deduplication
    .dropDuplicates(["sensor_id", "reading_timestamp"])

    # Rename
    .withColumnRenamed("_loaded_at", "loaded_at")

    # Select specific columns
    .select("sensor_id", "sensor_name", "equipment_name",
            "reading_value", "reading_timestamp", "status",
            "process_area", "is_valid", "loaded_at")
)

# Write to Silver
(df_transformed.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("petrova_prod.silver.sensor_readings_cleaned")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Complex Types (exam topic)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# Struct (nested columns)
df_struct = df.withColumn("sensor_info", F.struct(
    F.col("sensor_id"),
    F.col("sensor_name"),
    F.col("equipment_name")
))

# Array operations
df_array = (spark.sql("""
    SELECT
        sensor_id,
        COLLECT_LIST(reading_value) AS all_readings,
        ARRAY_DISTINCT(COLLECT_LIST(status)) AS unique_statuses,
        SIZE(COLLECT_LIST(reading_value)) AS reading_count
    FROM petrova_prod.bronze.sensor_readings
    GROUP BY sensor_id
"""))

# Explode (reverse of collect – exam topic)
df_exploded = df_array.select(
    "sensor_id",
    F.explode("all_readings").alias("single_reading")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Higher-Order Functions (exam topic)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRANSFORM: apply function to each array element
# MAGIC SELECT sensor_id,
# MAGIC        TRANSFORM(all_readings, x -> ROUND(x, 2)) AS rounded_readings
# MAGIC FROM sensor_arrays;
# MAGIC
# MAGIC -- FILTER: filter array elements
# MAGIC SELECT sensor_id,
# MAGIC        FILTER(all_readings, x -> x > 0) AS positive_readings
# MAGIC FROM sensor_arrays;
# MAGIC
# MAGIC -- REDUCE: aggregate array elements
# MAGIC SELECT sensor_id,
# MAGIC        REDUCE(all_readings, 0D, (acc, x) -> acc + x) AS total_reading
# MAGIC FROM sensor_arrays;
# MAGIC
# MAGIC -- EXISTS: check if any element matches condition
# MAGIC SELECT sensor_id,
# MAGIC        EXISTS(all_readings, x -> x > 9000) AS has_high_reading
# MAGIC FROM sensor_arrays;
