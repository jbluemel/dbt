{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    lower(email) as email,  -- Standardize email to lowercase
    upper(state) as state,  -- Standardize state to uppercase
    signup_date::date as signup_date
from {{ source('raw_data', 'customers') }}