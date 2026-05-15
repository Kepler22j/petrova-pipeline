{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi', 'revenue']
) }}

/*
  Gold Layer – Daily Revenue Fact (SAP Orders)
  Protected by Gold Immutability Commandments.
*/

WITH valid_orders AS (
    SELECT *
    FROM {{ ref('int_orders_validated') }}
    WHERE quality_flag IN ('PASS', 'WARN')
),

daily_rev AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_date::DATE', 'material_group']) }} AS revenue_sk,
        order_date::DATE                    AS revenue_date,
        material_group,
        COUNT(DISTINCT order_number)        AS order_count,
        SUM(order_quantity)                 AS total_quantity,
        SUM(net_value)                      AS total_revenue,
        AVG(net_value)                      AS avg_order_value,
        CURRENT_TIMESTAMP()                 AS _gold_loaded_at
    FROM valid_orders
    GROUP BY 1, 2, 3
)

SELECT * FROM daily_rev
