{{ config(materialized='table') }}

-- Get max week per fiscal year from actual data
with max_weeks_per_year as (
    select 
        "Year of Auction Endtime - Fiscal Year" as fiscal_year,
        max(
            case
                when extract(month from make_date(
                    "Year of Auction Endtime - Calendar Year"::int,
                    case "Month of Auction Endtime - Calendar Year"
                        when 'January' then 1 when 'February' then 2 when 'March' then 3
                        when 'April' then 4 when 'May' then 5 when 'June' then 6
                        when 'July' then 7 when 'August' then 8 when 'September' then 9
                        when 'October' then 10 when 'November' then 11 when 'December' then 12
                    end,
                    "Day of Auction Endtime - Calendar Year"::int
                )) >= 8 then
                    floor((make_date(
                        "Year of Auction Endtime - Calendar Year"::int,
                        case "Month of Auction Endtime - Calendar Year"
                            when 'January' then 1 when 'February' then 2 when 'March' then 3
                            when 'April' then 4 when 'May' then 5 when 'June' then 6
                            when 'July' then 7 when 'August' then 8 when 'September' then 9
                            when 'October' then 10 when 'November' then 11 when 'December' then 12
                        end,
                        "Day of Auction Endtime - Calendar Year"::int
                    ) - make_date("Year of Auction Endtime - Calendar Year"::int, 8, 1))::int / 7) + 1
                else
                    floor((make_date(
                        "Year of Auction Endtime - Calendar Year"::int,
                        case "Month of Auction Endtime - Calendar Year"
                            when 'January' then 1 when 'February' then 2 when 'March' then 3
                            when 'April' then 4 when 'May' then 5 when 'June' then 6
                            when 'July' then 7 when 'August' then 8 when 'September' then 9
                            when 'October' then 10 when 'November' then 11 when 'December' then 12
                        end,
                        "Day of Auction Endtime - Calendar Year"::int
                    ) - make_date("Year of Auction Endtime - Calendar Year"::int - 1, 8, 1))::int / 7) + 1
            end
        ) as max_week
    from {{ ref('itemv2') }}
    group by "Year of Auction Endtime - Fiscal Year"
),

-- Week spine: weeks 1 through max_week for each fiscal year
fiscal_week_spine as (
    select 
        m.fiscal_year,
        w.week_number as fiscal_week_number
    from max_weeks_per_year m
    cross join lateral (
        select generate_series(1, m.max_week::int) as week_number
    ) w
),

weekly_data as (
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
        "Contract Price" as contract_price
    from {{ ref('itemv2') }}
),

with_date as (
    select
        fiscal_year,
        make_date(calendar_year, month_num, day_num) as auction_date,
        contract_price
    from weekly_data
),

with_fiscal_week as (
    select
        fiscal_year,
        auction_date,
        contract_price,
        case
            when extract(month from auction_date) >= 8 then
                floor((auction_date - make_date(extract(year from auction_date)::int, 8, 1))::int / 7) + 1
            else
                floor((auction_date - make_date(extract(year from auction_date)::int - 1, 8, 1))::int / 7) + 1
        end as fiscal_week_number
    from with_date
),

actual_metrics as (
    select
        fiscal_year,
        fiscal_week_number,
        count(*) as total_items_sold,
        round(avg(contract_price)::numeric, 2) as avg_lot_value,
        sum(contract_price) as total_contract_price,
        sum(case when contract_price < 500 then 1 else 0 end) as items_under_500,
        round(100.0 * sum(case when contract_price < 500 then 1 else 0 end) / count(*), 2) as pct_items_under_500,
        sum(case when contract_price >= 10000 then 1 else 0 end) as items_10k_plus,
        round(100.0 * sum(case when contract_price >= 10000 then 1 else 0 end) / count(*), 2) as pct_items_10k_plus
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
)

select
    s.fiscal_year,
    s.fiscal_week_number,
    coalesce(m.total_items_sold, 0) as total_items_sold,
    coalesce(m.avg_lot_value, 0) as avg_lot_value,
    coalesce(m.total_contract_price, 0) as total_contract_price,
    coalesce(m.items_under_500, 0) as items_under_500,
    coalesce(m.pct_items_under_500, 0) as pct_items_under_500,
    coalesce(m.items_10k_plus, 0) as items_10k_plus,
    coalesce(m.pct_items_10k_plus, 0) as pct_items_10k_plus,
    case when m.total_items_sold is not null then true else false end as has_auctions
from fiscal_week_spine s
left join actual_metrics m 
    on s.fiscal_year = m.fiscal_year 
    and s.fiscal_week_number = m.fiscal_week_number
order by s.fiscal_year, s.fiscal_week_number