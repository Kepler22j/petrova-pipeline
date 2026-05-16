{{ config(
    materialized='ephemeral',
    tags=['silver', 'ephemeral-example']
) }}

/*
  Ephemeral model – compiled inline as CTE, never stored in database.
  Exam topic: ephemeral vs view vs table vs incremental.
  
  Use cases: lightweight transformations, intermediate logic shared
  between multiple downstream models without creating a database object.
*/

SELECT
    order_number,
    material_number,
    order_quantity,
    net_value,
    ROUND(net_value / NULLIF(order_quantity, 0), 2) AS unit_price,
    CASE
        WHEN net_value > 10000 THEN 'HIGH_VALUE'
        WHEN net_value > 1000  THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS order_tier
FROM {{ ref('stg_sap_orders') }}
WHERE order_quantity > 0
