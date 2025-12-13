{{ config(materialized='table') }}

-- Category Performance Model
-- Shows total winning bid amount for each category
-- Start simple, we can add more fields later!

with items as (
    select * from {{ ref('stg_items') }}
),

bids as (
    select * from {{ ref('stg_bids') }}
),

-- Find the winning bid (highest bid) for each item
winning_bids as (
    select
        item_id,
        max(bid_amount) as winning_bid_amount
    from bids
    group by item_id
),

-- Join items with their winning bids and aggregate by category
category_totals as (
    select
        i.category,
        sum(w.winning_bid_amount) as total_winning_bid_amount
    from items i
    inner join winning_bids w on i.item_id = w.item_id
    group by i.category
)

select
    category,
    total_winning_bid_amount
from category_totals
order by total_winning_bid_amount desc