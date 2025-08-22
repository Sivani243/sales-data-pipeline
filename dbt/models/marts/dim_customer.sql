-- {{ config(materialized='table') }}

-- with current as (
--     select *
--     from {{ ref('customers_snapshot') }}
--     where dbt_valid_to is null
-- ),

-- deduped as (
--     select
--         customer_id,
--         country,
--         status,
--         dbt_valid_from,
--         row_number() over (
--             partition by customer_id
--             order by dbt_valid_from desc
--         ) as rn
--     from current
-- )

-- select
--     customer_id,
--     country,
--     status,
--     dbt_valid_from as valid_from
-- from deduped
-- where rn = 1

{{ config(materialized='table') }}

with snapshot_data as (
    select
        customer_id,
        country,
        status,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('customers_snapshot') }}
)

select
    customer_id,
    country,
    status,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from snapshot_data
order by customer_id, dbt_valid_from