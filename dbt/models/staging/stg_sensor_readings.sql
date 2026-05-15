-- Bronze: Raw sensor readings (1:1 source mapping)
-- Source: Snowflake PETROVA_DW.PUBLIC.SENSOR_READINGS_PARQUET
-- Rule: NEVER modify. Audit trail and replay source.

{{ config(
    materialized='view',
    schema='bronze',
    tags=['bronze', 'sensors']
) }}

SELECT
    sensor_id,
    sensor_name,
    equipment_name,
    reading_value,
    reading_timestamp,
    status,
    process_area,
    _loaded_at AS loaded_at
FROM {{ source('raw', 'sensor_readings_parquet') }}
