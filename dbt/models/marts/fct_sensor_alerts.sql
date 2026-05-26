{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'alerts', 'monitoring'],
    pre_hook=["{{ log('Gold Gate: Sensor alert engine — stddev + lag + threshold', info=True) }}"]
) }}

/*
  Gold Layer – Sensor Alert Engine
  ════════════════════════════════
  Derives 6 alert categories from stddev + lag + threshold:

  1. STABILITY    — stddev thresholds (STABLE / NORMAL / UNSTABLE)
  2. SIGNAL_NOISE — coefficient of variation (stddev/avg > 0.3 = noisy)
  3. OUTLIER      — readings beyond 2x stddev from mean
  4. RANGE        — readings outside avg +/- 1 stddev band
  5. VOLATILITY   — stddev trending up vs previous day (degradation)
  6. SPIKE        — lag delta exceeds threshold (sudden jump)

  Why this works:
    stddev summarizes distribution spread → one metric, many interpretations.
    Combined with LAG (point-to-point change) and thresholds (business limits),
    you get a complete monitoring system from 3 statistical primitives.

  Interview note:
    This is Statistical Process Control (SPC) — same math used in
    manufacturing quality since Shewhart (1924). Not new, but proven.
*/

WITH kpi AS (
    SELECT *
    FROM {{ ref('fct_daily_sensor_kpi') }}
),

-- LAG: get previous day's values for trend + spike detection
with_lag AS (
    SELECT
        *,
        LAG(stddev_reading, 1) OVER (
            PARTITION BY sensor_id ORDER BY kpi_date
        ) AS prev_stddev,
        LAG(avg_reading, 1) OVER (
            PARTITION BY sensor_id ORDER BY kpi_date
        ) AS prev_avg,
        LAG(max_reading, 1) OVER (
            PARTITION BY sensor_id ORDER BY kpi_date
        ) AS prev_max
    FROM kpi
),

alerts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['sensor_id', 'kpi_date']) }} AS alert_sk,
        sensor_id,
        sensor_name,
        equipment_name,
        process_area,
        kpi_date,

        -- Raw metrics (for context)
        avg_reading,
        stddev_reading,
        min_reading,
        max_reading,
        total_readings,

        -- ═══ ALERT 1: STABILITY LEVEL ═══
        -- Direct stddev thresholds (tunable per sensor type)
        CASE
            WHEN stddev_reading IS NULL          THEN 'INSUFFICIENT_DATA'
            WHEN stddev_reading < 5              THEN 'STABLE'
            WHEN stddev_reading BETWEEN 5 AND 25 THEN 'NORMAL'
            WHEN stddev_reading > 25             THEN 'UNSTABLE'
        END AS stability_level,

        -- ═══ ALERT 2: SIGNAL QUALITY (Coefficient of Variation) ═══
        -- CV = stddev/avg — measures noise relative to signal magnitude
        -- CV > 0.3 means noise is 30%+ of signal = unreliable
        CASE
            WHEN avg_reading = 0 OR avg_reading IS NULL THEN 'INVALID'
            WHEN stddev_reading / NULLIF(ABS(avg_reading), 0) > 0.5 THEN 'VERY_NOISY'
            WHEN stddev_reading / NULLIF(ABS(avg_reading), 0) > 0.3 THEN 'NOISY'
            ELSE 'CLEAN'
        END AS signal_quality,
        ROUND(stddev_reading / NULLIF(ABS(avg_reading), 0), 4) AS coefficient_of_variation,

        -- ═══ ALERT 3: OUTLIER FLAG ═══
        -- Any reading beyond 2 stddev from mean = statistical outlier
        CASE
            WHEN max_reading > avg_reading + (2 * stddev_reading) THEN TRUE
            WHEN min_reading < avg_reading - (2 * stddev_reading) THEN TRUE
            ELSE FALSE
        END AS has_outlier,

        -- ═══ ALERT 4: RANGE VIOLATION ═══
        -- Expected band = avg +/- 1 stddev (68% of normal readings)
        ROUND(avg_reading - stddev_reading, 2) AS expected_lower,
        ROUND(avg_reading + stddev_reading, 2) AS expected_upper,
        CASE
            WHEN min_reading < (avg_reading - stddev_reading)
              OR max_reading > (avg_reading + stddev_reading) THEN TRUE
            ELSE FALSE
        END AS range_violated,

        -- ═══ ALERT 5: VOLATILITY TREND ═══
        -- Is stddev increasing day-over-day? (system degradation signal)
        prev_stddev,
        CASE
            WHEN prev_stddev IS NULL THEN 'NO_HISTORY'
            WHEN stddev_reading > prev_stddev * 1.5 THEN 'DEGRADING'
            WHEN stddev_reading > prev_stddev * 1.1 THEN 'INCREASING'
            WHEN stddev_reading < prev_stddev * 0.9 THEN 'IMPROVING'
            ELSE 'STEADY'
        END AS volatility_trend,

        -- ═══ ALERT 6: SPIKE DETECTION (LAG-based) ═══
        -- Sudden jump in max or avg vs previous day
        CASE
            WHEN prev_max IS NULL THEN 'NO_HISTORY'
            WHEN ABS(max_reading - prev_max) > 2 * COALESCE(stddev_reading, 0) THEN 'SPIKE'
            WHEN ABS(avg_reading - COALESCE(prev_avg, avg_reading)) > stddev_reading THEN 'SHIFT'
            ELSE 'NORMAL'
        END AS spike_status,

        -- ═══ COMPOSITE SEVERITY ═══
        -- Combine all signals into one priority level
        CASE
            WHEN stddev_reading > 25
             AND stddev_reading / NULLIF(ABS(avg_reading), 0) > 0.3
                THEN 'CRITICAL'
            WHEN stddev_reading > 25
              OR max_reading > avg_reading + (2 * stddev_reading)
              OR (prev_stddev IS NOT NULL AND stddev_reading > prev_stddev * 1.5)
                THEN 'WARNING'
            ELSE 'OK'
        END AS alert_severity,

        {{ dbt.current_timestamp() }} AS _alert_generated_at

    FROM with_lag
)

SELECT * FROM alerts
