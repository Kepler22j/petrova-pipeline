{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi', 'revenue']
) }}

/*
  Gold Layer – Daily Revenue Fact (SAP Orders)
  ════════════════════════════════════════════
  Aggregates validated orders by date and material group.
  Protected by Gold Immutability Commandments.
  Only PASS/WARN records from Silver qualify.
*/

WITH valid_orders AS (
    SELECT *
    FROM {{ ref('int_orders_validated') }}
    WHERE quality_flag IN ('PASS', 'WARN')
),

daily_rev AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_date::DATE', 'equipment_type']) }} AS revenue_sk,
        order_date::DATE                    AS revenue_date,
        equipment_type,
        equipment_location,
        COUNT(DISTINCT order_id)            AS order_count,
        SUM(COALESCE(net_value, 0))         AS total_revenue,
        AVG(NULLIF(net_value, 0))           AS avg_order_value,
        SUM(COALESCE(order_quantity, 0))    AS total_quantity,

        -- Quality metrics
        SUM(CASE WHEN quality_flag = 'WARN' THEN 1 ELSE 0 END) AS warn_count,
        COUNT(*)                            AS total_records,

        CURRENT_TIMESTAMP()                 AS _gold_loaded_at
    FROM valid_orders
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM daily_rev
