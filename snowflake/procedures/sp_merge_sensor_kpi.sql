-- ============================================================
-- PETROVA – MERGE Procedure: Bronze → Silver → Gold Sensor KPI
-- Implements Gold Immutability Commandment #3: MERGE-only writes
-- ============================================================

CREATE OR REPLACE PROCEDURE PETROVA_PROD.GOLD.SP_MERGE_SENSOR_KPI()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Step 1: MERGE into Gold from Silver aggregation
    MERGE INTO PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI AS tgt
    USING (
        SELECT
            MD5(sensor_id || '|' || reading_timestamp::DATE) AS kpi_sk,
            sensor_id,
            sensor_name,
            equipment_name,
            process_area,
            reading_timestamp::DATE AS kpi_date,
            COUNT(*)                            AS total_readings,
            AVG(reading_value)                  AS avg_reading,
            MIN(reading_value)                  AS min_reading,
            MAX(reading_value)                  AS max_reading,
            STDDEV(reading_value)               AS stddev_reading,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY reading_value) AS median_reading,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY reading_value) AS p95_reading,
            SUM(CASE WHEN quality_flag = 'WARN' THEN 1 ELSE 0 END) AS warn_count
        FROM PETROVA_PROD.SILVER.SENSOR_READINGS_CLEANED
        WHERE quality_flag IN ('PASS', 'WARN') AND is_valid = TRUE
        GROUP BY 1, 2, 3, 4, 5, 6
    ) AS src
    ON tgt.kpi_sk = src.kpi_sk

    WHEN MATCHED THEN UPDATE SET
        tgt.total_readings  = src.total_readings,
        tgt.avg_reading     = src.avg_reading,
        tgt.min_reading     = src.min_reading,
        tgt.max_reading     = src.max_reading,
        tgt.stddev_reading  = src.stddev_reading,
        tgt.median_reading  = src.median_reading,
        tgt.p95_reading     = src.p95_reading,
        tgt.warn_count      = src.warn_count,
        tgt._gold_loaded_at = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        kpi_sk, sensor_id, sensor_name, equipment_name, process_area,
        kpi_date, total_readings, avg_reading, min_reading, max_reading,
        stddev_reading, median_reading, p95_reading, warn_count
    ) VALUES (
        src.kpi_sk, src.sensor_id, src.sensor_name, src.equipment_name, src.process_area,
        src.kpi_date, src.total_readings, src.avg_reading, src.min_reading, src.max_reading,
        src.stddev_reading, src.median_reading, src.p95_reading, src.warn_count
    );

    -- Step 2: Audit log
    INSERT INTO PETROVA_PROD.AUDIT.PIPELINE_LOG (procedure_name, status, row_count, executed_at)
    SELECT 'SP_MERGE_SENSOR_KPI', 'SUCCESS', COUNT(*), CURRENT_TIMESTAMP()
    FROM PETROVA_PROD.GOLD.FCT_DAILY_SENSOR_KPI;

    RETURN 'SP_MERGE_SENSOR_KPI completed successfully';
END;
$$;
