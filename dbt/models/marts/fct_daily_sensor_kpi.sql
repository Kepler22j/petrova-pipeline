{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'kpi', 'sensors'],
    pre_hook=["{{ log('Gold Gate: FMEA validation for sensor KPIs', info=True) }}"]
) }}

/*
  Gold Layer – Daily Sensor KPI Fact Table
  Business rule: Only PASS/WARN readings qualify for KPI aggregation.
  Immutability: Protected by RBAC (GOLD_READER / GOLD_WRITER roles).
  Ref: Agent.MD Section 6 – Gold Immutability 7 Commandments
*/

WITH valid_readings AS (
    SELECT *
    FROM {{ ref('int_sensor_cleaned') }}
    WHERE quality_flag IN ('PASS', 'WARN')
      AND is_valid = TRUE
),

daily_agg AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['sensor_id', 'reading_timestamp::DATE']) }} AS kpi_sk,
        sensor_id,
        sensor_name,
        equipment_name,
        process_area,
        reading_timestamp::DATE AS kpi_date,

        -- KPI metrics
        COUNT(*)                            AS total_readings,
        AVG(reading_value)                  AS avg_reading,
        MIN(reading_value)                  AS min_reading,
        MAX(reading_value)                  AS max_reading,
        STDDEV(reading_value)               AS stddev_reading,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY reading_value) AS median_reading,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY reading_value) AS p95_reading,

        -- Quality summary
        SUM(CASE WHEN quality_flag = 'WARN' THEN 1 ELSE 0 END) AS warn_count,

        CURRENT_TIMESTAMP() AS _gold_loaded_at
    FROM valid_readings
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM daily_agg
