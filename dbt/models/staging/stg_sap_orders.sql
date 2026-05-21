-- Bronze: Raw SAP maintenance orders (1:1 source mapping)
{{ config(materialized='view', schema='bronze', tags=['bronze', 'sap']) }}

SELECT
    order_id,
    order_type,
    equipment_id,
    material_id,
    order_date,
    order_quantity,
    net_value,
    status,
    priority,
    created_by,
    _loaded_at AS loaded_at
FROM {{ source('raw', 'raw_sap_orders') }}
