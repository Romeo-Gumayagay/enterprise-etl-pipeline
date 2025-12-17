{{
  config(
    materialized='table',
    schema='analytics',
    indexes=[
      {'columns': ['customer_id'], 'type': 'btree', 'unique': true},
      {'columns': ['email'], 'type': 'btree'}
    ]
  )
}}

with customer_metrics as (
    select * from {{ ref('int_customer_orders') }}
),

customer_segments as (
    select
        *,
        case
            when total_spent >= 10000 then 'VIP'
            when total_spent >= 5000 then 'Premium'
            when total_spent >= 1000 then 'Standard'
            else 'New'
        end as customer_segment,
        
        case
            when total_orders >= 20 then 'High Frequency'
            when total_orders >= 10 then 'Medium Frequency'
            when total_orders >= 1 then 'Low Frequency'
            else 'No Orders'
        end as order_frequency_segment,
        
        case
            when customer_lifetime_days >= 365 then 'Long-term'
            when customer_lifetime_days >= 180 then 'Medium-term'
            when customer_lifetime_days >= 30 then 'Short-term'
            else 'New'
        end as customer_tenure_segment
        
    from customer_metrics
)

select * from customer_segments

