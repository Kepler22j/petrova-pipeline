{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='equipment_sk',
    tags=['silver', 'equipment']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_equipment_master') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['equipment_id']) }} AS equipment_sk,
        equipment_id,
        equipment_name,
        equipment_type,
        location,
        install_date,
        status,
        loaded_at,

        -- Derived fields
        {{ dbt.datediff('install_date', dbt.current_timestamp(), 'day') }} AS days_since_install,
        CASE
            WHEN {{ dbt.datediff('install_date', dbt.current_timestamp(), 'year') }} > 10 THEN 'AGING'
            WHEN status = 'DECOMMISSIONED' THEN 'RETIRED'
            ELSE 'ACTIVE'
        END AS lifecycle_stage,

        {{ dbt.current_timestamp() }} AS _silver_loaded_at
    FROM source
)

SELECT * FROM enriched
