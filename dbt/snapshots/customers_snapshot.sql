{% snapshot customers_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}

-- Derive latest customer attributes from staging
select
  customer_id,
  country,
  status,
  updated_at
from {{ ref('src_orders') }}

{% endsnapshot %}