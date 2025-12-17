{{
  config(
    materialized='view',
    schema='intermediate'
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_order_metrics as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.registration_date,
        c.status as customer_status,
        
        -- Order metrics
        count(distinct o.order_id) as total_orders,
        sum(case when o.order_status = 'COMPLETED' then 1 else 0 end) as completed_orders,
        sum(case when o.order_status = 'CANCELLED' then 1 else 0 end) as cancelled_orders,
        sum(o.total_amount) as total_spent,
        avg(o.total_amount) as avg_order_value,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        
        -- Time-based metrics
        datediff('day', min(o.order_date), max(o.order_date)) as customer_lifetime_days,
        datediff('day', c.registration_date, min(o.order_date)) as days_to_first_order
        
    from customers c
    left join orders o
        on c.customer_id = o.customer_id
    group by 1, 2, 3, 4, 5, 6
)

select * from customer_order_metrics

