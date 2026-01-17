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
        taxonomy_family,
        taxonomy_category,
        item_region_id,
        item_region_name,
        item_district_id,
        item_territory_id,
        business_category,
        contract_price,
        buyer_premium,
        seller_service_fee,
        lot_fee
    from {{ ref('items') }}
    where safe_for_alv = true
      and is_sold = true
)

select
    fiscal_year,
    fiscal_week_number,
    taxonomy_industry,
    taxonomy_family,
    taxonomy_category,
    item_region_id,
    item_region_name,
    item_district_id,
    item_territory_id,
    business_category,
    count(*) as total_items_sold,
    round(avg(contract_price)::numeric, 2) as avg_lot_value,
    sum(contract_price) as total_contract_price,
    sum(buyer_premium) + sum(seller_service_fee) + sum(lot_fee) as auction_revenue,
    sum(case when contract_price < 500 then 1 else 0 end) as items_under_500,
    round(100.0 * sum(case when contract_price < 500 then 1 else 0 end) / count(*), 2) as pct_items_under_500,
    sum(case when contract_price >= 10000 then 1 else 0 end) as items_10k_plus,
    round(100.0 * sum(case when contract_price >= 10000 then 1 else 0 end) / count(*), 2) as pct_items_10k_plus
from weekly_data
group by 
    fiscal_year,
    fiscal_week_number,
    taxonomy_industry,
    taxonomy_family,
    taxonomy_category,
    item_region_id,
    item_region_name,
    item_district_id,
    item_territory_id,
    business_category
order by fiscal_year, fiscal_week_number