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
        DATEDIFF('day', install_date, CURRENT_DATE()) AS days_since_install,
        CASE
            WHEN DATEDIFF('year', install_date, CURRENT_DATE()) > 10 THEN 'AGING'
            WHEN status = 'DECOMMISSIONED' THEN 'RETIRED'
            ELSE 'ACTIVE'
        END AS lifecycle_stage,

        CURRENT_TIMESTAMP() AS _silver_loaded_at
    FROM source
)

SELECT * FROM enriched
