{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source_data as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        registration_date,
        status,
        created_at,
        updated_at
    from {{ source('raw_data', 'customers') }}
    where registration_date >= '{{ var("start_date") }}'
),

cleaned_data as (
    select
        customer_id,
        trim(upper(first_name)) as first_name,
        trim(upper(last_name)) as last_name,
        lower(trim(email)) as email,
        regexp_replace(phone, '[^0-9]', '') as phone,
        registration_date,
        status,
        created_at,
        updated_at,
        current_timestamp() as dbt_loaded_at
    from source_data
    where email is not null
      and email like '%@%.%'
)

select * from cleaned_data

