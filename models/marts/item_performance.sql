{{ config(materialized='table') }}

select
    i.item_id,
    i.item_name,
    i.category,
    i.starting_bid,
    i.auction_date,
    count(b.bid_id) as total_bids,
    max(b.bid_amount) as final_price,
    max(b.bid_amount) - i.starting_bid as price_increase,
    round((max(b.bid_amount) - i.starting_bid) / i.starting_bid * 100, 2) as price_increase_pct
from {{ ref('stg_items') }} i
left join {{ ref('stg_bids') }} b
    on i.item_id = b.item_id
group by i.item_id, i.item_name, i.category, i.starting_bid, i.auction_date