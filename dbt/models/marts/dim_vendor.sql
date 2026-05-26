{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'vendor', 'scd2']
) }}

/*
  Gold Layer – Vendor Dimension (SCD2 current view)
  Full history in Silver; Gold exposes current state + history flag.
*/

SELECT
    vendor_sk,
    vendor_number,
    vendor_name,
    country,
    payment_terms,
    is_current,
    valid_from,
    valid_to,
    {{ dbt.current_timestamp() }} AS _gold_loaded_at
FROM {{ ref('scd2_vendors') }}
