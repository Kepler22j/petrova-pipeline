{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='sensor_reading_sk',
    tags=['silver', 'sensors', 'quality-gate']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

validated AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['sensor_id', 'reading_timestamp']) }} AS sensor_reading_sk,
        sensor_id,
        sensor_name,
        equipment_name,
        reading_value,
        reading_timestamp,
        status,
        process_area,
        loaded_at,

        -- Silver Gate: quality flag via the 3-gate framework macro
        -- (warn band [-999, 9999]; NULL -> FAIL). See macros/three_gate_validation.sql
        {{ silver_quality_flag('reading_value', -999, 9999) }} AS quality_flag,
        {{ silver_is_valid('reading_value', -999, 9999) }} AS is_valid,

        {{ dbt.current_timestamp() }} AS _silver_loaded_at
    FROM source
)

SELECT * FROM validated
