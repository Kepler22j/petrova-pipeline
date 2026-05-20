-- ============================================================
-- PETROVA – UDFs & Stored Procedures (SnowPro Core Exam)
-- Topics: SQL UDF, JavaScript UDF, Python UDF, UDTF, Secure UDF
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA GOLD;

-- 1. SQL Scalar UDF
CREATE OR REPLACE FUNCTION QUALITY_SCORE(pass_count INT, total_count INT)
    RETURNS FLOAT
    LANGUAGE SQL
AS
$$
    CASE WHEN total_count = 0 THEN 0
         ELSE ROUND((pass_count::FLOAT / total_count) * 100, 2)
    END
$$;

-- Usage: SELECT QUALITY_SCORE(148500, 150000);  -- Returns 99.00

-- 2. JavaScript UDF (exam topic)
CREATE OR REPLACE FUNCTION PARSE_SENSOR_STATUS(raw_status VARCHAR)
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
AS
$$
    if (!RAW_STATUS) return 'UNKNOWN';
    var s = RAW_STATUS.toUpperCase().trim();
    var mapping = {'A': 'ACTIVE', 'I': 'INACTIVE', 'M': 'MAINTENANCE', 'E': 'ERROR'};
    return mapping[s] || s;
$$;

-- 3. Python UDF (exam topic – Snowpark)
CREATE OR REPLACE FUNCTION CALCULATE_ANOMALY_SCORE(reading FLOAT, avg_reading FLOAT, stddev_reading FLOAT)
    RETURNS FLOAT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    HANDLER = 'anomaly_score'
AS
$$
def anomaly_score(reading, avg_reading, stddev_reading):
    if stddev_reading is None or stddev_reading == 0:
        return 0.0
    z_score = abs((reading - avg_reading) / stddev_reading)
    return round(z_score, 4)
$$;

-- 4. Table Function (UDTF) – returns multiple rows
CREATE OR REPLACE FUNCTION GENERATE_DATE_RANGE(start_date DATE, end_date DATE)
    RETURNS TABLE(dt DATE)
    LANGUAGE SQL
AS
$$
    SELECT DATEADD('day', seq4(), start_date) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', start_date, end_date) + 1))
$$;

-- Usage: SELECT * FROM TABLE(GENERATE_DATE_RANGE('2025-01-01', '2025-01-31'));

-- 5. Secure UDF (exam topic: hides definition from non-owners)
CREATE OR REPLACE SECURE FUNCTION GOLD.REVENUE_TIER(revenue FLOAT)
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
    CASE
        WHEN revenue >= 1000000 THEN 'PLATINUM'
        WHEN revenue >= 500000  THEN 'GOLD'
        WHEN revenue >= 100000  THEN 'SILVER'
        ELSE 'BRONZE'
    END
$$;
