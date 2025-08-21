with base as (
  select * from {{ ref('src_orders') }}
  where status = 'completed'
)
select
  order_id,
  order_line_id,
  order_ts,
  customer_id,
  product_id,
  quantity,
  unit_price,
  currency,
  country,
  status,
  updated_at,
  (quantity * unit_price)::numeric(14,2) as line_amount
from base
