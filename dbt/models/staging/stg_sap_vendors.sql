-- Bronze: Raw SAP vendors (1:1 source mapping)
{{ config(materialized='view', schema='bronze', tags=['bronze', 'sap']) }}

SELECT
    vendor_id,
    vendor_name,
    vendor_type,
    country,
    payment_terms,
    _loaded_at AS loaded_at
FROM {{ source('raw', 'raw_sap_vendors') }}
