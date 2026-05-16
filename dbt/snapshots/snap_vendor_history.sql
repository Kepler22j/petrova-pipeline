{% snapshot snap_vendor_history %}
{#
  dbt Snapshot – SCD Type 2 for vendor dimension
  Exam topic: snapshot strategy (timestamp vs check), invalidate_hard_deletes
  This is the dbt-native alternative to our custom scd2_merge macro.
#}

{{
    config(
        target_schema='silver',
        unique_key='vendor_number',
        strategy='timestamp',
        updated_at='loaded_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    vendor_number,
    vendor_name,
    country,
    payment_terms,
    loaded_at
FROM {{ source('raw', 'sap_vendors_raw') }}

{% endsnapshot %}
