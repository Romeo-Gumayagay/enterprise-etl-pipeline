-- Test: All customer emails should be valid format
select 
    customer_id,
    email
from {{ ref('stg_customers') }}
where email not like '%@%.%'
   or email is null

