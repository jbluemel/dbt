{{ config(materialized='table') }}

with weekly_data as (
    select
        "Year of Auction Endtime - Fiscal Year" as fiscal_year,
        case "Month of Auction Endtime - Calendar Year"
            when 'January' then 1
            when 'February' then 2
            when 'March' then 3
            when 'April' then 4
            when 'May' then 5
            when 'June' then 6
            when 'July' then 7
            when 'August' then 8
            when 'September' then 9
            when 'October' then 10
            when 'November' then 11
            when 'December' then 12
        end as month_num,
        "Year of Auction Endtime - Calendar Year"::int as calendar_year,
        "Day of Auction Endtime - Calendar Year"::int as day_num,
        "Item Territory Id" as item_territory_id,
        "Item District" as item_district,
        "Item Region Id" as item_region_id,
        "Contract Price" as contract_price
    from {{ ref('itemv2') }}
),

with_date as (
    select
        fiscal_year,
        make_date(calendar_year, month_num, day_num) as auction_date,
        item_territory_id,
        item_district,
        item_region_id,
        contract_price
    from weekly_data
),

with_fiscal_week as (
    select
        fiscal_year,
        item_territory_id,
        item_district,
        item_region_id,
        contract_price,
        case
            when extract(month from auction_date) >= 8 then
                floor((auction_date - make_date(extract(year from auction_date)::int, 8, 1))::int / 7) + 1
            else
                floor((auction_date - make_date(extract(year from auction_date)::int - 1, 8, 1))::int / 7) + 1
        end as fiscal_week_number
    from with_date
)

select
    fiscal_year,
    fiscal_week_number,
    item_territory_id,
    item_district,
    item_region_id,
    count(*) as total_items_sold,
    round(avg(contract_price)::numeric, 2) as avg_lot_value,
    sum(contract_price) as total_contract_price,
    sum(case when contract_price < 500 then 1 else 0 end) as items_under_500,
    round(100.0 * sum(case when contract_price < 500 then 1 else 0 end) / count(*), 2) as pct_items_under_500,
    sum(case when contract_price >= 10000 then 1 else 0 end) as items_10k_plus,
    round(100.0 * sum(case when contract_price >= 10000 then 1 else 0 end) / count(*), 2) as pct_items_10k_plus
from with_fiscal_week
group by fiscal_year, fiscal_week_number, item_territory_id, item_district, item_region_id
order by fiscal_year, fiscal_week_number, item_territory_id