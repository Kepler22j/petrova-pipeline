-- Bronze: Equipment master data (1:1 source mapping)
{{ config(materialized='view', schema='bronze', tags=['bronze', 'equipment']) }}

SELECT
    equipment_id,
    equipment_name,
    equipment_type,
    location,
    install_date,
    manufacturer,
    _loaded_at AS loaded_at
FROM {{ source('raw', 'raw_equipment_master') }}
