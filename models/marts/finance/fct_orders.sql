{{
  config(
    materialized='table',
    schema='analytics',
    indexes=[
      {'columns': ['order_date'], 'type': 'btree'},
      {'columns': ['customer_id'], 'type': 'btree'},
      {'columns': ['order_status'], 'type': 'btree'}
    ]
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Order facts
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.total_amount,
        o.currency,
        o.payment_method,
        
        -- Customer dimensions
        c.first_name,
        c.last_name,
        c.email,
        c.registration_date as customer_registration_date,
        c.status as customer_status,
        
        -- Time dimensions
        date_part('year', o.order_date) as order_year,
        date_part('quarter', o.order_date) as order_quarter,
        date_part('month', o.order_date) as order_month,
        date_part('dayofweek', o.order_date) as order_day_of_week,
        
        -- Calculated fields
        case 
            when o.order_status = 'COMPLETED' then o.total_amount 
            else 0 
        end as revenue,
        
        case 
            when o.order_status = 'CANCELLED' then o.total_amount 
            else 0 
        end as cancelled_amount,
        
        -- Metadata
        o.created_at,
        o.updated_at,
        current_timestamp() as dbt_loaded_at
        
    from orders o
    inner join customers c
        on o.customer_id = c.customer_id
    where o.order_date >= '{{ var("start_date") }}'
)

select * from final

