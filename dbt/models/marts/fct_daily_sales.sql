{#
  Incremental daily fact:
  - unique_key: sales_date (one row per day)
  - strategy: delete+insert (Postgres-friendly)
  - late-arriving handling via var('late_arriving_days', 2)
#}

{{ config(
    materialized='incremental',
    unique_key='sales_date',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns',
    post_hook="create index if not exists idx_{{ this.name }}_sales_date on {{ this }} (sales_date)"
) }}

{% set lookback_days = var('late_arriving_days', 2) %}

with base as (
    select
        date_trunc('day', order_ts)::date as sales_date,
        count(distinct order_id)           as orders,
        sum(quantity)                      as units_sold,
        sum(quantity * unit_price)         as gross_revenue
    from {{ ref('orders_enriched') }}
    where 1=1
    {% if is_incremental() %}
      and date_trunc('day', order_ts)::date >= (
        coalesce( (select max(sales_date) from {{ this }}), '1900-01-01'::date )
        - interval '{{ lookback_days }} day'
      )
    {% endif %}
    group by 1
)
select * from base