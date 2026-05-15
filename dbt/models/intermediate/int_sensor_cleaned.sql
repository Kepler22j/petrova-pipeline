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

        -- Silver Gate: quality flags
        CASE
            WHEN reading_value IS NULL THEN 'FAIL'
            WHEN reading_value < -999 OR reading_value > 9999 THEN 'WARN'
            ELSE 'PASS'
        END AS quality_flag,

        CASE
            WHEN reading_value IS NOT NULL
                 AND reading_value BETWEEN -999 AND 9999
            THEN TRUE ELSE FALSE
        END AS is_valid,

        CURRENT_TIMESTAMP() AS _silver_loaded_at
    FROM source
)

SELECT * FROM validated
