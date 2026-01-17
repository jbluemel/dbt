{{ config(materialized='table') }}

with item_data as (
    select
        auction_date,
        taxonomy_category,
        contract_price,
        buyer_premium,
        seller_service_fee
    from {{ ref('items') }}
    where safe_for_alv = true
),

with_fiscal as (
    select
        *,
        -- Purple Wave fiscal year starts August 1
        case 
            when extract(month from auction_date) >= 8 
            then extract(year from auction_date) + 1
            else extract(year from auction_date)
        end as fiscal_year,
        -- Fiscal week calculation (weeks since August 1)
        floor(
            (auction_date::date - (date_trunc('year', auction_date - interval '7 months') + interval '7 months')::date) / 7
        ) + 1 as fiscal_week_number
    from item_data
)

select
    fiscal_year,
    fiscal_week_number,
    taxonomy_category,
    count(*) as total_items_sold,
    round(avg(contract_price)::numeric, 2) as avg_lot_value,
    sum(contract_price) as total_contract_price,
    sum(buyer_premium + seller_service_fee) as auction_revenue,
    sum(case when contract_price < 500 then 1 else 0 end) as items_under_500,
    round(100.0 * sum(case when contract_price < 500 then 1 else 0 end) / count(*), 2) as pct_items_under_500,
    sum(case when contract_price >= 10000 then 1 else 0 end) as items_10k_plus,
    round(100.0 * sum(case when contract_price >= 10000 then 1 else 0 end) / count(*), 2) as pct_items_10k_plus
from with_fiscal
group by fiscal_year, fiscal_week_number, taxonomy_category
order by fiscal_year, fiscal_week_number, taxonomy_category