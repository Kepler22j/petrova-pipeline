-- ============================================================
-- PETROVA 300K – Streams & Change Tracking (SnowPro Core Exam)
-- Topics: Standard/Append-only streams, CHANGES clause, CDC
-- ============================================================
USE DATABASE PETROVA_PROD;

-- 1. Standard Stream (tracks INSERT, UPDATE, DELETE)
CREATE STREAM IF NOT EXISTS BRONZE.STM_SENSOR_CHANGES
    ON TABLE BRONZE.SENSOR_READINGS_PARQUET
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on raw sensor data – tracks all DML';

-- 2. Append-only Stream (tracks INSERT only – more efficient)
CREATE STREAM IF NOT EXISTS BRONZE.STM_SENSOR_INSERTS
    ON TABLE BRONZE.SENSOR_READINGS_PARQUET
    APPEND_ONLY = TRUE
    COMMENT = 'Append-only stream for new sensor inserts';

-- 3. Stream metadata columns (exam frequently tests these)
-- METADATA$ACTION    = 'INSERT' or 'DELETE'
-- METADATA$ISUPDATE  = TRUE if the row is part of an UPDATE
-- METADATA$ROW_ID    = Unique row identifier

-- 4. Consume stream in a MERGE (standard CDC pattern)
MERGE INTO SILVER.SENSOR_READINGS_CLEANED AS tgt
USING (
    SELECT
        MD5(sensor_id || '|' || reading_timestamp) AS sensor_reading_sk,
        sensor_id,
        sensor_name,
        equipment_name,
        reading_value,
        reading_timestamp,
        status,
        process_area,
        _loaded_at AS loaded_at,
        METADATA$ACTION AS stream_action,
        METADATA$ISUPDATE AS is_update
    FROM BRONZE.STM_SENSOR_CHANGES
    WHERE METADATA$ACTION = 'INSERT'
) AS src
ON tgt.sensor_reading_sk = src.sensor_reading_sk
WHEN NOT MATCHED THEN INSERT (
    sensor_reading_sk, sensor_id, sensor_name, equipment_name,
    reading_value, reading_timestamp, status, process_area, loaded_at
) VALUES (
    src.sensor_reading_sk, src.sensor_id, src.sensor_name, src.equipment_name,
    src.reading_value, src.reading_timestamp, src.status, src.process_area, src.loaded_at
);

-- 5. Stream on a View (exam topic)
CREATE VIEW IF NOT EXISTS BRONZE.V_RECENT_ORDERS AS
    SELECT * FROM BRONZE.SAP_ORDERS_RAW WHERE order_date >= '2025-01-01';

CREATE STREAM IF NOT EXISTS BRONZE.STM_RECENT_ORDERS
    ON VIEW BRONZE.V_RECENT_ORDERS
    COMMENT = 'Stream on a view – tracks changes to filtered orders';

-- 6. Check if stream has data (exam topic)
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STM_SENSOR_CHANGES') AS has_data;

-- 7. Task consuming a stream (common exam pattern)
CREATE TASK IF NOT EXISTS BRONZE.TASK_PROCESS_SENSOR_CDC
    WAREHOUSE = PETROVA_ETL_WH
    SCHEDULE  = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STM_SENSOR_INSERTS')
AS
    INSERT INTO SILVER.SENSOR_READINGS_CLEANED
    SELECT
        MD5(sensor_id || '|' || reading_timestamp),
        sensor_id, sensor_name, equipment_name,
        reading_value, reading_timestamp, status, process_area,
        CASE WHEN reading_value IS NULL THEN 'FAIL'
             WHEN reading_value < -999 OR reading_value > 9999 THEN 'WARN'
             ELSE 'PASS' END,
        reading_value IS NOT NULL AND reading_value BETWEEN -999 AND 9999,
        _loaded_at,
        CURRENT_TIMESTAMP()
    FROM BRONZE.STM_SENSOR_INSERTS;

-- 8. CHANGES clause (alternative to streams – exam topic)
ALTER TABLE BRONZE.SAP_ORDERS_RAW SET CHANGE_TRACKING = TRUE;

SELECT *
FROM BRONZE.SAP_ORDERS_RAW
    CHANGES(INFORMATION => DEFAULT)
    AT(OFFSET => -3600);  -- Changes in the last hour
