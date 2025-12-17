-- Test: All order amounts should be positive
select 
    order_id,
    total_amount
from {{ ref('stg_orders') }}
where total_amount < 0

