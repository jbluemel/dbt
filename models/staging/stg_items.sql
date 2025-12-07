{{ config(materialized='view') }}

select
    item_id,
    item_name,
    category,
    starting_bid,
    auction_date::date as auction_date
from {{ source('raw_data', 'item') }}