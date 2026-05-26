{% macro scd2_merge(target_relation, source_relation, unique_key, tracked_columns) %}
{#
  Generic SCD Type 2 MERGE macro for Snowflake.
  Mirrors SSIS Lookup + MERGE pattern from PETROVA Agent.MD Section 5.

  Args:
    target_relation  – the target table ref
    source_relation  – CTE or ref with incoming rows
    unique_key       – natural/business key column name
    tracked_columns  – list of columns to track for changes
#}

MERGE INTO {{ target_relation }} AS tgt
USING (
    SELECT
        *,
        MD5(CONCAT_WS('|', {{ tracked_columns | join(', ') }})) AS _row_hash
    FROM {{ source_relation }}
) AS src
ON tgt.{{ unique_key }} = src.{{ unique_key }}
   AND tgt.is_current = TRUE

-- Existing row changed → expire it
WHEN MATCHED AND tgt._row_hash != src._row_hash THEN
    UPDATE SET
        tgt.is_current = FALSE,
        tgt.valid_to   = {{ dbt.current_timestamp() }}

-- New row → insert
WHEN NOT MATCHED THEN
    INSERT (
        {{ unique_key }},
        {% for col in tracked_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
        is_current, valid_from, valid_to, _row_hash
    )
    VALUES (
        src.{{ unique_key }},
        {% for col in tracked_columns %}src.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %},
        TRUE, {{ dbt.current_timestamp() }}, '9999-12-31', src._row_hash
    );

{% endmacro %}
