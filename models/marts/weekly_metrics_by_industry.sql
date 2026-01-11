{{ config(materialized='table') }}

with weekly_data as (
    select
        case 
            when extract(month from auction_date) >= 8 
            then extract(year from auction_date) + 1
            else extract(year from auction_date)
        end as fiscal_year,
        
        case
            when extract(month from auction_date) >= 8
            then floor((auction_date - make_date(extract(year from auction_date)::int, 8, 1)) / 7) + 1
            else floor((auction_date - make_date(extract(year from auction_date)::int - 1, 8, 1)) / 7) + 1
        end as fiscal_week_number,
        
        taxonomy_industry,
        contract_price,
        item_id
    from {{ ref('items') }}
    where safe_for_alv = true
      and is_sold = true
)

select
    fiscal_year,
    fiscal_week_number,
    taxonomy_industry,
    count(*) as total_items_sold,
    round(avg(contract_price)::numeric, 2) as avg_lot_value,
    sum(contract_price) as total_contract_price
from weekly_data
group by fiscal_year, fiscal_week_number, taxonomy_industry
order by fiscal_year, fiscal_week_number, taxonomy_industry