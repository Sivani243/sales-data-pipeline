{{ config(materialized='incremental', unique_key='sales_date') }}

with lines as (
  select * from {{ ref('orders_enriched') }}
)

select
  date_trunc('day', order_ts)::date as sales_date,
  count(distinct order_id)          as orders,
  sum(quantity)                     as units_sold,
  sum(line_amount)::numeric(16,2)   as gross_revenue
from lines
{% if is_incremental() %}
where order_ts::date > (select coalesce(max(sales_date), date '1900-01-01') from {{ this }})
{% endif %}
group by 1
