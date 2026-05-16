-- ============================================================
-- PETROVA 300K – Semi-Structured Data (SnowPro Core Exam)
-- Topics: VARIANT, OBJECT, ARRAY, FLATTEN, LATERAL, PARSE_JSON
-- ============================================================
USE DATABASE PETROVA_PROD;
USE SCHEMA BRONZE;

-- 1. Table with VARIANT column for raw JSON/Parquet
CREATE TABLE IF NOT EXISTS IOT_EVENTS_RAW (
    event_id        VARCHAR(50)     NOT NULL,
    event_timestamp TIMESTAMP_NTZ   NOT NULL,
    raw_payload     VARIANT,                        -- Semi-structured JSON
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _source_file    VARCHAR(500)
)
COMMENT = 'Raw IoT events stored as VARIANT for schema-on-read';

-- 2. Ingesting JSON into VARIANT
COPY INTO IOT_EVENTS_RAW (event_id, event_timestamp, raw_payload, _source_file)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:timestamp::TIMESTAMP_NTZ,
        $1,                                         -- Entire JSON as VARIANT
        METADATA$FILENAME
    FROM @STG_AZURE_LANDING/iot_events/ (FILE_FORMAT => 'JSON_FORMAT')
);

-- 3. File Formats for semi-structured data
CREATE FILE FORMAT IF NOT EXISTS JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    COMMENT = 'Standard JSON ingestion format';

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY
    COMMENT = 'Parquet format for columnar data';

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    COMMENT = 'Standard CSV with header skip';

-- 4. Querying semi-structured data with dot notation
SELECT
    event_id,
    raw_payload:sensor_id::VARCHAR          AS sensor_id,       -- Dot notation
    raw_payload:readings[0]:value::FLOAT    AS first_reading,   -- Array access
    raw_payload:metadata.location::VARCHAR  AS location,        -- Nested object
    raw_payload:tags::ARRAY                 AS tags_array       -- Cast to ARRAY
FROM IOT_EVENTS_RAW
WHERE raw_payload:status::VARCHAR = 'ACTIVE'
LIMIT 100;

-- 5. FLATTEN – Explode nested arrays into rows
-- Key exam topic: LATERAL FLATTEN for denormalization
SELECT
    e.event_id,
    e.event_timestamp,
    f.value:sensor_id::VARCHAR              AS sensor_id,
    f.value:reading_value::FLOAT            AS reading_value,
    f.value:unit::VARCHAR                   AS unit,
    f.index                                 AS reading_index    -- Position in array
FROM IOT_EVENTS_RAW e,
    LATERAL FLATTEN(input => e.raw_payload:readings) f
WHERE e.event_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP());

-- 6. FLATTEN nested objects (key-value pairs)
SELECT
    event_id,
    kv.key                                  AS attribute_name,
    kv.value::VARCHAR                       AS attribute_value
FROM IOT_EVENTS_RAW,
    LATERAL FLATTEN(input => raw_payload:metadata) kv;

-- 7. PARSE_JSON and OBJECT_CONSTRUCT
SELECT
    PARSE_JSON('{"name": "sensor_01", "value": 42.5}') AS json_obj,
    OBJECT_CONSTRUCT(
        'sensor_id', sensor_id,
        'avg_reading', AVG(reading_value),
        'count', COUNT(*)
    ) AS summary_json
FROM SILVER.SENSOR_READINGS_CLEANED
GROUP BY sensor_id;

-- 8. ARRAY functions
SELECT
    ARRAY_CONSTRUCT('PASS', 'WARN', 'FAIL')         AS status_array,
    ARRAY_SIZE(raw_payload:readings)                 AS num_readings,
    ARRAY_CONTAINS('ERROR'::VARIANT, raw_payload:tags) AS has_error_tag,
    ARRAY_AGG(DISTINCT sensor_id) WITHIN GROUP (ORDER BY sensor_id) AS all_sensors
FROM IOT_EVENTS_RAW
GROUP BY raw_payload:readings, raw_payload:tags;

-- 9. TYPE checking functions
SELECT
    event_id,
    TYPEOF(raw_payload)                     AS payload_type,     -- OBJECT
    TYPEOF(raw_payload:readings)            AS readings_type,    -- ARRAY
    TYPEOF(raw_payload:readings[0]:value)   AS value_type,       -- DECIMAL
    IS_NULL_VALUE(raw_payload:optional_field) AS is_json_null    -- JSON null check
FROM IOT_EVENTS_RAW
LIMIT 10;
