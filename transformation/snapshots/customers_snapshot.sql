{% snapshot customers_snapshot %}
{{
    config(
        target_schema = 'SNAPSHOTS',
        unique_key    = 'customer_id',
        strategy      = 'check',
        check_cols    = ['city', 'state', 'zip_code_prefix'],
    )
}}

-- Exclude dbt_updated_at to avoid conflict with dbt's own snapshot columns
SELECT
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    city,
    state
FROM {{ source('olist_raw', 'dim_customers') }}

{% endsnapshot %}
