{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      PETROVA schema routing:
        - staging  → BRONZE
        - intermediate → SILVER
        - marts    → GOLD
      In production, use the custom_schema_name directly (no prefix).
      In dev, prefix with target schema to avoid collisions.
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name in ['prod', 'local'] -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
