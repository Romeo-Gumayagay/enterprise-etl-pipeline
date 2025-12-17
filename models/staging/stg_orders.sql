{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        currency,
        payment_method,
        shipping_address,
        created_at,
        updated_at
    from {{ source('raw_data', 'orders') }}
    where order_date >= '{{ var("start_date") }}'
),

cleaned_data as (
    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        upper(trim(order_status)) as order_status,
        cast(total_amount as decimal(18, 2)) as total_amount,
        upper(trim(currency)) as currency,
        payment_method,
        shipping_address,
        created_at,
        updated_at,
        current_timestamp() as dbt_loaded_at
    from source_data
    where customer_id is not null
      and order_date is not null
      and total_amount >= 0
)

select * from cleaned_data

