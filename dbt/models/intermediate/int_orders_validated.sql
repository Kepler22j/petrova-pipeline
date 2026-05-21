{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='order_sk',
    tags=['silver', 'orders', 'quality-gate']
) }}

/*
  Silver Layer – Validated Maintenance Orders
  ═══════════════════════════════════════════
  Joins SAP orders with equipment master for enrichment.
  Silver Gate: validates order completeness and business rules.

  Gate 1: Null detection (order_date, equipment_id)
  Gate 2: Status filtering, dedup by order_id
  Gate 3: Referential integrity (equipment_id must exist)
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_sap_orders') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

-- Gate 2: Dedup – keep latest record per order_id
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY loaded_at DESC
        ) AS _rn
    FROM source
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_sk,
        o.order_id,
        o.order_type,
        o.equipment_id,
        o.order_date,
        o.status,
        o.priority,
        o.created_by,
        o.loaded_at,

        -- Enrichment from equipment master (Gate 3: referential integrity)
        e.equipment_name,
        e.equipment_type,
        e.location AS equipment_location,

        -- Gate 1: Null detection + Gate 2: Business rules
        CASE
            WHEN o.order_date IS NULL THEN 'FAIL'
            WHEN o.equipment_id IS NULL THEN 'FAIL'
            WHEN o.status = 'CANCELLED' THEN 'FAIL'
            WHEN e.equipment_id IS NULL THEN 'WARN'   -- orphan order
            WHEN o.priority IS NULL THEN 'WARN'
            ELSE 'PASS'
        END AS quality_flag,

        -- Derived: days since order
        DATEDIFF('day', o.order_date, CURRENT_DATE()) AS days_since_order,

        CURRENT_TIMESTAMP() AS _silver_loaded_at

    FROM deduped o
    LEFT JOIN {{ ref('stg_equipment_master') }} e
        ON o.equipment_id = e.equipment_id
    WHERE o._rn = 1  -- dedup: latest only
)

SELECT * FROM enriched
