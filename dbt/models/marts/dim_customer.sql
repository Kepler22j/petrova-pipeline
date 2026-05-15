{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimension', 'customer']
) }}

/*
  Gold Layer – Customer Dimension (placeholder)
  TODO: Wire to SAP customer master via stg_sap_customers when source available.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(["'PLACEHOLDER'"]) }} AS customer_sk,
    'PLACEHOLDER' AS customer_id,
    'Sample Customer' AS customer_name,
    'US' AS country,
    'ACTIVE' AS status,
    CURRENT_TIMESTAMP() AS _gold_loaded_at
