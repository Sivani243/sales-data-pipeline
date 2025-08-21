select
  cast(order_id as bigint)              as order_id,
  cast(order_line_id as bigint)         as order_line_id,
  cast(order_ts as timestamp)           as order_ts,
  cast(customer_id as bigint)           as customer_id,
  cast(product_id as bigint)            as product_id,
  cast(quantity as integer)             as quantity,
  cast(unit_price as numeric(12,2))     as unit_price,
  cast(currency as varchar(3))          as currency,
  cast(country as varchar(64))          as country,
  cast(status as varchar(16))           as status,
  cast(updated_at as timestamp)         as updated_at
from raw.orders
