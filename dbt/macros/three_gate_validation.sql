{#
  PETROVA — 3-Gate Validation Framework
  =====================================
  Reusable macros implementing the Bronze -> Silver -> Gold quality gates.
  These are now wired DIRECTLY into the medallion models, so the framework
  is enforced in compiled SQL — not just documented.

    Bronze Gate  — schema / null completeness     (staging + intermediate)
    Silver Gate  — business range + quality flag   (int_sensor_cleaned, int_orders_validated)
    Gold  Gate   — SPC / FMEA risk block           (fct_* pre/post-hook)

  Ref: Agent.MD Section 4 — 3-Gate Validation
#}


{# ─────────────────────────────────────────────────────────────────────
   SILVER GATE — per-column quality flag (FAIL / WARN / PASS)

   NULL                          -> FAIL   (cannot be trusted)
   outside [fail_lo, fail_hi]    -> FAIL   (physically impossible reading)
   outside [warn_lo, warn_hi]    -> WARN   (suspicious — keep but flag)
   otherwise                     -> PASS

   `fail_lo` / `fail_hi` are optional: omit for a WARN-only gate.
   Usage:  {{ silver_quality_flag('reading_value', -999, 9999) }} AS quality_flag
   ───────────────────────────────────────────────────────────────────── #}
{% macro silver_quality_flag(value_col, warn_lo, warn_hi, fail_lo=none, fail_hi=none) -%}
    CASE
        WHEN {{ value_col }} IS NULL THEN 'FAIL'
        {%- if fail_lo is not none and fail_hi is not none %}
        WHEN {{ value_col }} < {{ fail_lo }} OR {{ value_col }} > {{ fail_hi }} THEN 'FAIL'
        {%- endif %}
        WHEN {{ value_col }} < {{ warn_lo }} OR {{ value_col }} > {{ warn_hi }} THEN 'WARN'
        ELSE 'PASS'
    END
{%- endmacro %}


{# is_valid boolean — TRUE only when the reading sits inside the acceptable band #}
{% macro silver_is_valid(value_col, lo, hi) -%}
    CASE
        WHEN {{ value_col }} IS NOT NULL
             AND {{ value_col }} BETWEEN {{ lo }} AND {{ hi }}
        THEN TRUE ELSE FALSE
    END
{%- endmacro %}


{# ─────────────────────────────────────────────────────────────────────
   BRONZE GATE — schema / null completeness
   Single PASS/FAIL column based on required-not-null columns.
   Usage:  {{ bronze_schema_gate(['sensor_id', 'reading_timestamp']) }} AS _bronze_gate
   ───────────────────────────────────────────────────────────────────── #}
{% macro bronze_schema_gate(required_cols) -%}
    CASE
        {%- for col in required_cols %}
        WHEN {{ col }} IS NULL THEN 'FAIL'
        {%- endfor %}
        ELSE 'PASS'
    END
{%- endmacro %}


{# ─────────────────────────────────────────────────────────────────────
   GOLD GATE — SPC severity block
   Blocks promotion when composite SPC severity is CRITICAL.
   Usage:  {{ gold_gate_spc('alert_severity') }} AS _gold_gate
   ───────────────────────────────────────────────────────────────────── #}
{% macro gold_gate_spc(severity_col) -%}
    CASE
        WHEN {{ severity_col }} = 'CRITICAL' THEN 'BLOCK'
        WHEN {{ severity_col }} = 'WARNING'  THEN 'REVIEW'
        ELSE 'APPROVE'
    END
{%- endmacro %}


{# GOLD GATE (alt) — FMEA risk block. RPN = severity x occurrence x detection (1–1000). #}
{% macro gold_fmea_gate(rpn_col, threshold=100) -%}
    CASE WHEN {{ rpn_col }} > {{ threshold }} THEN 'BLOCK' ELSE 'APPROVE' END
{%- endmacro %}
