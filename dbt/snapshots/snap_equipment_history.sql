{% snapshot snap_equipment_history %}
{#
  dbt Snapshot with check strategy (exam topic)
  Compares actual column values instead of relying on a timestamp.
#}

{{
    config(
        target_schema='silver',
        unique_key='equipment_id',
        strategy='check',
        check_cols=['equipment_name', 'equipment_type', 'location', 'status'],
        invalidate_hard_deletes=False
    )
}}

SELECT
    equipment_id,
    equipment_name,
    equipment_type,
    location,
    status,
    install_date
FROM {{ source('raw', 'equipment_master_raw') }}

{% endsnapshot %}
