# Databricks notebook source
# MAGIC %md
# MAGIC # PETROVA – Delta Live Tables Pipeline (Databricks DE Associate Exam)
# MAGIC Topics: @dlt.table, @dlt.view, expectations, Medallion in DLT

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# ═══ BRONZE LAYER ═══

@dlt.table(
    name="bronze_sensor_readings",
    comment="Raw sensor readings ingested via Auto Loader",
    table_properties={"quality": "bronze"}
)
def bronze_sensor_readings():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/petrova/landing/sensors/")
    )


@dlt.table(
    name="bronze_sap_orders",
    comment="Raw SAP orders",
    table_properties={"quality": "bronze"}
)
def bronze_sap_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/mnt/petrova/landing/sap/orders/")
    )


# ═══ SILVER LAYER (with expectations = quality gates) ═══

@dlt.table(
    name="silver_sensor_cleaned",
    comment="Cleaned sensor readings with quality validation",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_sensor_id", "sensor_id IS NOT NULL")          # Warn but don't drop
@dlt.expect("valid_timestamp", "reading_timestamp IS NOT NULL")   # Warn but don't drop
@dlt.expect_or_drop("valid_reading_range", "reading_value BETWEEN -999 AND 9999")  # Drop invalid
@dlt.expect_or_fail("has_required_fields", "sensor_id IS NOT NULL AND reading_timestamp IS NOT NULL")  # Fail pipeline
def silver_sensor_cleaned():
    return (
        dlt.read_stream("bronze_sensor_readings")
        .withColumn("quality_flag",
            F.when(F.col("reading_value").isNull(), "FAIL")
             .when(~F.col("reading_value").between(-999, 9999), "WARN")
             .otherwise("PASS")
        )
        .withColumn("is_valid",
            (F.col("reading_value").isNotNull()) &
            (F.col("reading_value").between(-999, 9999))
        )
    )


@dlt.table(
    name="silver_orders_validated",
    comment="Validated SAP orders",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_quantity", "order_quantity > 0")
def silver_orders_validated():
    return (
        dlt.read_stream("bronze_sap_orders")
        .withColumn("quality_flag",
            F.when(F.col("order_quantity") <= 0, "FAIL")
             .when(F.col("net_value").isNull(), "WARN")
             .otherwise("PASS")
        )
    )


# ═══ GOLD LAYER ═══

@dlt.table(
    name="gold_daily_sensor_kpi",
    comment="Daily sensor KPI aggregations",
    table_properties={"quality": "gold"}
)
def gold_daily_sensor_kpi():
    return (
        dlt.read("silver_sensor_cleaned")
        .filter(F.col("quality_flag").isin("PASS", "WARN"))
        .groupBy(
            "sensor_id", "sensor_name", "equipment_name", "process_area",
            F.to_date("reading_timestamp").alias("kpi_date")
        )
        .agg(
            F.count("*").alias("total_readings"),
            F.avg("reading_value").alias("avg_reading"),
            F.min("reading_value").alias("min_reading"),
            F.max("reading_value").alias("max_reading"),
            F.stddev("reading_value").alias("stddev_reading"),
            F.sum(F.when(F.col("quality_flag") == "WARN", 1).otherwise(0)).alias("warn_count")
        )
    )


# ═══ DLT Views (exam topic: temporary, not persisted) ═══

@dlt.view(
    name="v_sensor_summary",
    comment="Temporary view – not stored in metastore"
)
def v_sensor_summary():
    return (
        dlt.read("silver_sensor_cleaned")
        .groupBy("sensor_id")
        .agg(F.count("*").alias("total_count"))
    )
