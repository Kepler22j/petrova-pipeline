{% macro audit_columns() %}
    CURRENT_TIMESTAMP()   AS _dbt_loaded_at,
    '{{ invocation_id }}' AS _dbt_invocation_id,
    '{{ target.name }}'   AS _dbt_target
{% endmacro %}

{% macro add_audit_columns(model_sql) %}
    SELECT
        source_query.*,
        {{ audit_columns() }}
    FROM (
        {{ model_sql }}
    ) AS source_query
{% endmacro %}
