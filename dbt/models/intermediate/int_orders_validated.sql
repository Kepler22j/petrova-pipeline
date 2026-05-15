{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='order_sk',
    tags=['silver', 'orders', 'quality-gate']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_sap_orders') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_number']) }} AS order_sk,
        o.*,
        m.material_description,
        m.material_group,
        m.unit_of_measure,

        -- Silver Gate: validation
        CASE
            WHEN o.order_quantity <= 0 THEN 'FAIL'
            WHEN o.net_value IS NULL THEN 'WARN'
            ELSE 'PASS'
        END AS quality_flag,

        CURRENT_TIMESTAMP() AS _silver_loaded_at
    FROM source o
    LEFT JOIN {{ ref('stg_sap_materials') }} m
        ON o.material_number = m.material_number
)

SELECT * FROM enriched
