{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='vendor_sk',
    tags=['silver', 'scd2', 'vendors'],
    post_hook=[
        "UPDATE {{ this }} SET is_current = FALSE, valid_to = CURRENT_TIMESTAMP()
         WHERE vendor_number IN (
            SELECT vendor_number FROM {{ this }}
            GROUP BY vendor_number HAVING COUNT(*) > 1
         ) AND is_current = TRUE
           AND vendor_sk != (
              SELECT MAX(vendor_sk) FROM {{ this }} t2
              WHERE t2.vendor_number = {{ this }}.vendor_number
           )"
    ]
) }}

/*
  SCD Type 2 implementation for vendor dimension.
  Mirrors the SSIS Lookup + Snowflake MERGE pattern from Agent.MD Section 5.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_sap_vendors') }}
),

hashed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['vendor_number', 'vendor_name', 'country', 'payment_terms']) }} AS vendor_sk,
        vendor_number,
        vendor_name,
        country,
        payment_terms,
        loaded_at,
        {{ dbt_utils.generate_surrogate_key(['vendor_name', 'country', 'payment_terms']) }} AS row_hash,
        TRUE AS is_current,
        loaded_at AS valid_from,
        CAST('9999-12-31' AS TIMESTAMP) AS valid_to,
        CURRENT_TIMESTAMP() AS _silver_loaded_at
    FROM source
)

{% if is_incremental() %}
SELECT h.*
FROM hashed h
LEFT JOIN {{ this }} t
    ON h.vendor_number = t.vendor_number AND t.is_current = TRUE
WHERE t.vendor_number IS NULL          -- new vendor
   OR t.row_hash != h.row_hash         -- changed vendor (new SCD2 row)
{% else %}
SELECT * FROM hashed
{% endif %}
