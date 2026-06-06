-- ============================================================
-- PETROVA – Time Travel & Fail-safe (SnowPro Core Exam)
-- Topics: AT/BEFORE, UNDROP, DATA_RETENTION_TIME_IN_DAYS
-- ============================================================
USE DATABASE PETROVA_PROD;

-- 1. Time Travel with AT (exact point in time)
SELECT * FROM GOLD.FCT_DAILY_SENSOR_KPI
    AT(TIMESTAMP => '2025-06-01 00:00:00'::TIMESTAMP);

-- 2. Time Travel with BEFORE (just before a statement)
SELECT * FROM GOLD.FCT_DAILY_SENSOR_KPI
    BEFORE(STATEMENT => '<query_id>');  -- Replace with actual query ID

-- 3. Time Travel with OFFSET (relative seconds)
SELECT * FROM GOLD.FCT_DAILY_SENSOR_KPI
    AT(OFFSET => -3600);  -- 1 hour ago

-- 4. Set data retention period (exam: Enterprise = 90 days max, Standard = 1 day)
ALTER TABLE GOLD.FCT_DAILY_SENSOR_KPI
    SET DATA_RETENTION_TIME_IN_DAYS = 90;

ALTER SCHEMA GOLD
    SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- 5. UNDROP – Recover dropped objects
-- DROP TABLE GOLD.FCT_DAILY_SENSOR_KPI;
-- UNDROP TABLE GOLD.FCT_DAILY_SENSOR_KPI;

-- DROP SCHEMA GOLD;
-- UNDROP SCHEMA GOLD;

-- DROP DATABASE PETROVA_PROD;
-- UNDROP DATABASE PETROVA_PROD;

-- 6. CREATE TABLE AS SELECT from Time Travel (disaster recovery)
CREATE OR REPLACE TABLE GOLD.FCT_DAILY_SENSOR_KPI_RECOVERED AS
    SELECT * FROM GOLD.FCT_DAILY_SENSOR_KPI
    AT(OFFSET => -7200);  -- 2 hours ago

-- 7. Zero-Copy Clone from Time Travel point
CREATE OR REPLACE TABLE GOLD.FCT_DAILY_SENSOR_KPI_CLONE
    CLONE GOLD.FCT_DAILY_SENSOR_KPI
    AT(TIMESTAMP => '2025-06-01 00:00:00'::TIMESTAMP);

-- 8. Fail-safe (exam topic: 7 days AFTER Time Travel expires)
-- Fail-safe is NOT user-accessible – Snowflake support only
-- Time Travel period: configurable (0-90 days for Enterprise)
-- Fail-safe period: always 7 days (non-configurable)
-- Transient/Temporary tables: NO fail-safe (0 days)

-- 9. Transient & Temporary tables (exam topic: reduced storage costs)
CREATE TRANSIENT TABLE STAGING.TEMP_IMPORT (
    id INT,
    data VARIANT
)
DATA_RETENTION_TIME_IN_DAYS = 0;  -- Transient: 0 or 1 day only, no fail-safe

CREATE TEMPORARY TABLE SESSION_SCRATCH (
    id INT,
    data VARCHAR
);  -- Temporary: exists only for session duration, no fail-safe
