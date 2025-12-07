{{ config(materialized='table') }}

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.state,
    count(b.bid_id) as total_bids,
    sum(case when b.is_winning_bid then 1 else 0 end) as winning_bids,
    sum(case when b.is_winning_bid then b.bid_amount else 0 end) as total_spent
from {{ ref('stg_customers') }} c
left join {{ ref('stg_bids') }} b
    on c.customer_id = b.customer_id
group by c.customer_id, c.first_name, c.last_name, c.email, c.state