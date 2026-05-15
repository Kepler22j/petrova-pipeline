-- ============================================================
-- PETROVA – External Stages & Snowflake Tasks
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA BRONZE;

-- Azure Blob external stage for Parquet ingestion
CREATE STAGE IF NOT EXISTS STG_AZURE_LANDING
    URL = 'azure://petrovadatalake.blob.core.windows.net/landing/'
    STORAGE_INTEGRATION = PETROVA_AZURE_INTEGRATION
    FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY);

-- Snowpipe for auto-ingest sensor data
CREATE PIPE IF NOT EXISTS PIPE_SENSOR_INGEST
    AUTO_INGEST = TRUE
    AS
    COPY INTO BRONZE.SENSOR_READINGS_PARQUET
    FROM @STG_AZURE_LANDING/sensors/
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Scheduled Snowflake Task: run Gold MERGE daily at 03:00 UTC
CREATE TASK IF NOT EXISTS TASK_GOLD_SENSOR_KPI
    WAREHOUSE = PETROVA_ETL_WH
    SCHEDULE  = 'USING CRON 0 3 * * * UTC'
    COMMENT   = 'Daily Gold KPI MERGE after Airflow pipeline completes'
AS
    CALL PETROVA_PROD.GOLD.SP_MERGE_SENSOR_KPI();

-- Audit table
USE SCHEMA AUDIT;
CREATE TABLE IF NOT EXISTS PIPELINE_LOG (
    log_id          NUMBER AUTOINCREMENT,
    procedure_name  VARCHAR(200),
    status          VARCHAR(20),
    row_count       INTEGER,
    executed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    error_message   VARCHAR(2000)
);
