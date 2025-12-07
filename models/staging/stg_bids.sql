{{ config(materialized='view') }}

select
    bid_id,
    item_id,
    customer_id,
    bid_amount,
    bid_timestamp::timestamp as bid_timestamp,
    is_winning_bid::boolean as is_winning_bid
from {{ source('raw_data', 'bids') }}