{% macro bronze_gate(column_list) %}
{#
  Bronze Gate: Schema validation – ensure required columns are non-null.
  Ref: Agent.MD Section 4 – 3-Gate Validation
#}
{% for col in column_list %}
    CASE WHEN {{ col }} IS NULL THEN 'FAIL' ELSE 'PASS' END AS {{ col }}_check,
{% endfor %}
    CASE
        {% for col in column_list %}
        WHEN {{ col }} IS NULL THEN 'FAIL'
        {% endfor %}
        ELSE 'PASS'
    END AS _bronze_gate_result
{% endmacro %}

{% macro silver_gate(quality_rules) %}
{#
  Silver Gate: Data quality validation.
  quality_rules: list of dicts with {column, test, threshold}
#}
{% for rule in quality_rules %}
    CASE
        WHEN {{ rule.column }} {{ rule.test }} {{ rule.threshold }} THEN 'FAIL'
        ELSE 'PASS'
    END AS {{ rule.column }}_quality_check,
{% endfor %}
    'EVALUATED' AS _silver_gate_result
{% endmacro %}

{% macro gold_gate_fmea(risk_column, threshold=100) %}
{#
  Gold Gate: FMEA risk assessment.
  Blocks promotion if risk priority number exceeds threshold.
#}
    CASE
        WHEN {{ risk_column }} > {{ threshold }} THEN 'BLOCK'
        ELSE 'APPROVE'
    END AS _gold_gate_fmea_result
{% endmacro %}
