-- Bronze: Raw SAP materials (1:1 source mapping)
{{ config(materialized='view', schema='bronze', tags=['bronze', 'sap']) }}

SELECT
    material_id,
    material_name,
    material_type,
    material_type AS material_group,   -- alias for downstream grouping
    unit_of_measure,
    plant_code,
    _loaded_at AS loaded_at
FROM {{ source('raw', 'raw_sap_materials') }}
