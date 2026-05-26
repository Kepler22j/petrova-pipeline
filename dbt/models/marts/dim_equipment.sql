{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'equipment']
) }}

/*
  Gold Layer – Equipment Dimension
  Source: Silver int_equipment_enriched
*/

SELECT
    equipment_sk,
    equipment_id,
    equipment_name,
    equipment_type,
    location,
    install_date,
    status,
    days_since_install,
    lifecycle_stage,
    {{ dbt.current_timestamp() }} AS _gold_loaded_at
FROM {{ ref('int_equipment_enriched') }}
