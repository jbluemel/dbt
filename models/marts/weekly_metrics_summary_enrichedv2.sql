{{ config(materialized='table') }}

with base_data as (
    select
        "Year of Auction Endtime - Fiscal Year" as fiscal_year,
        make_date(
            "Year of Auction Endtime - Calendar Year"::int,
            case "Month of Auction Endtime - Calendar Year"
                when 'January' then 1 when 'February' then 2 when 'March' then 3
                when 'April' then 4 when 'May' then 5 when 'June' then 6
                when 'July' then 7 when 'August' then 8 when 'September' then 9
                when 'October' then 10 when 'November' then 11 when 'December' then 12
            end,
            "Day of Auction Endtime - Calendar Year"::int
        ) as auction_date,
        "Taxonomy Industry" as taxonomy_industry,
        "Item Region Id" as item_region_id,
        "Contract Price" as contract_price
    from public.itemv2
    where "Contract Price" is not null
),

with_fiscal_week as (
    select
        *,
        case
            when extract(month from auction_date) >= 8 then
                floor((auction_date - make_date(extract(year from auction_date)::int, 8, 1))::int / 7) + 1
            else
                floor((auction_date - make_date(extract(year from auction_date)::int - 1, 8, 1))::int / 7) + 1
        end as fiscal_week_number
    from base_data
),

-- Base weekly summary
weekly_summary as (
    select
        fiscal_year,
        fiscal_week_number,
        count(*) as total_items_sold,
        avg(contract_price) as avg_lot_value,
        sum(contract_price) as total_contract_price,
        round((100.0 * sum(case when contract_price >= 10000 then 1 else 0 end) / count(*))::numeric, 2) as pct_items_10k_plus,
        round((100.0 * sum(case when contract_price < 500 then 1 else 0 end) / count(*))::numeric, 2) as pct_items_under_500,
        true as has_auctions
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
),

-- Industry mix
industry_mix as (
    select
        fiscal_year,
        fiscal_week_number,
        count(*) as week_total,
        sum(case when taxonomy_industry = 'Construction Equipment' then 1 else 0 end) as construction_items,
        sum(case when taxonomy_industry = 'Passenger Vehicles, Boats and RVs' then 1 else 0 end) as passenger_items,
        sum(case when taxonomy_industry = 'Trucks, Medium and Heavy Duty' then 1 else 0 end) as trucks_items,
        sum(case when taxonomy_industry = 'Ag Equipment' then 1 else 0 end) as ag_items
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
),

-- Industry ALV
industry_alv as (
    select
        fiscal_year,
        fiscal_week_number,
        avg(case when taxonomy_industry = 'Construction Equipment' then contract_price end) as construction_alv,
        avg(case when taxonomy_industry = 'Trucks, Medium and Heavy Duty' then contract_price end) as trucks_alv,
        avg(case when taxonomy_industry = 'Passenger Vehicles, Boats and RVs' then contract_price end) as passenger_alv,
        avg(case when taxonomy_industry = 'Ag Equipment' then contract_price end) as ag_alv
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
),

-- Region mix
region_mix as (
    select
        fiscal_year,
        fiscal_week_number,
        count(*) as region_week_total,
        sum(case when item_region_id = 3 then 1 else 0 end) as region_3_items,
        sum(case when item_region_id = 4 then 1 else 0 end) as region_4_items,
        sum(case when item_region_id = 5 then 1 else 0 end) as region_5_items
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
),

-- Region ALV
region_alv as (
    select
        fiscal_year,
        fiscal_week_number,
        avg(case when item_region_id = 3 then contract_price end) as region_3_alv,
        avg(case when item_region_id = 4 then contract_price end) as region_4_alv,
        avg(case when item_region_id = 5 then contract_price end) as region_5_alv
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number
),

-- Industry totals for top revenue and anomaly detection
industry_weekly as (
    select
        fiscal_year,
        fiscal_week_number,
        taxonomy_industry,
        count(*) as total_items_sold,
        avg(contract_price) as avg_lot_value,
        sum(contract_price) as total_contract_price
    from with_fiscal_week
    group by fiscal_year, fiscal_week_number, taxonomy_industry
),

-- Top revenue contributor
industry_with_rank as (
    select
        *,
        sum(total_contract_price) over (partition by fiscal_year, fiscal_week_number) as week_total_revenue,
        row_number() over (partition by fiscal_year, fiscal_week_number order by total_contract_price desc) as revenue_rank
    from industry_weekly
),

top_revenue as (
    select
        fiscal_year,
        fiscal_week_number,
        taxonomy_industry as top_revenue_industry,
        round((100.0 * total_contract_price / nullif(week_total_revenue, 0))::numeric, 2) as top_revenue_pct,
        round(avg_lot_value::numeric, 2) as top_revenue_alv
    from industry_with_rank
    where revenue_rank = 1
),

-- Anomaly detection
industry_benchmarks as (
    select * from (values
        ('Construction Equipment', 14639),
        ('Trucks, Medium and Heavy Duty', 15536),
        ('Passenger Vehicles, Boats and RVs', 6190),
        ('Ag Equipment', 7282),
        ('Trailers', 8194),
        ('Support Equipment', 2281),
        ('Forklifts and Material Handling', 3676),
        ('Business and Personal Property', 1747),
        ('Tools, Tires and Parts', 1287),
        ('Forestry and Logging Equipment', 16946),
        ('Building Materials', 2356)
    ) as t(taxonomy_industry, benchmark_alv)
),

anomaly_candidates as (
    select
        i.fiscal_year,
        i.fiscal_week_number,
        i.taxonomy_industry,
        i.avg_lot_value,
        i.total_contract_price,
        b.benchmark_alv,
        round((100.0 * (i.avg_lot_value - b.benchmark_alv) / nullif(b.benchmark_alv, 0))::numeric, 1) as pct_above_benchmark,
        row_number() over (
            partition by i.fiscal_year, i.fiscal_week_number 
            order by (i.avg_lot_value - b.benchmark_alv) / nullif(b.benchmark_alv, 0) desc
        ) as anomaly_rank
    from industry_weekly i
    join industry_benchmarks b on i.taxonomy_industry = b.taxonomy_industry
    where i.taxonomy_industry not in (
        'Construction Equipment', 
        'Trucks, Medium and Heavy Duty', 
        'Passenger Vehicles, Boats and RVs', 
        'Ag Equipment'
    )
    and i.avg_lot_value > b.benchmark_alv * 1.3
    and i.total_items_sold >= 5
),

anomalies as (
    select
        fiscal_year,
        fiscal_week_number,
        taxonomy_industry as anomaly_industry,
        round(avg_lot_value::numeric, 2) as anomaly_alv,
        pct_above_benchmark as anomaly_pct_above_typical
    from anomaly_candidates
    where anomaly_rank = 1
)

select
    s.fiscal_year,
    s.fiscal_week_number,
    s.total_items_sold,
    round(s.avg_lot_value::numeric, 2) as avg_lot_value,
    round(s.total_contract_price::numeric, 2) as total_contract_price,
    s.pct_items_10k_plus,
    s.pct_items_under_500,
    s.has_auctions,
    
    -- Construction metrics
    coalesce(round((100.0 * i.construction_items / nullif(i.week_total, 0))::numeric, 2), 0) as construction_pct,
    round(a.construction_alv::numeric, 2) as construction_alv,
    
    -- Passenger metrics
    coalesce(round((100.0 * i.passenger_items / nullif(i.week_total, 0))::numeric, 2), 0) as passenger_pct,
    round(a.passenger_alv::numeric, 2) as passenger_alv,
    
    -- Trucks metrics
    coalesce(round((100.0 * i.trucks_items / nullif(i.week_total, 0))::numeric, 2), 0) as trucks_med_heavy_pct,
    round(a.trucks_alv::numeric, 2) as trucks_med_heavy_alv,
    
    -- Ag metrics
    coalesce(round((100.0 * i.ag_items / nullif(i.week_total, 0))::numeric, 2), 0) as ag_pct,
    round(a.ag_alv::numeric, 2) as ag_alv,
    
    -- Region metrics
    coalesce(round((100.0 * r.region_3_items / nullif(r.region_week_total, 0))::numeric, 2), 0) as region_3_pct,
    round(ra.region_3_alv::numeric, 2) as region_3_alv,
    coalesce(round((100.0 * r.region_4_items / nullif(r.region_week_total, 0))::numeric, 2), 0) as region_4_pct,
    round(ra.region_4_alv::numeric, 2) as region_4_alv,
    coalesce(round((100.0 * r.region_5_items / nullif(r.region_week_total, 0))::numeric, 2), 0) as region_5_pct,
    round(ra.region_5_alv::numeric, 2) as region_5_alv,
    
    -- Top revenue contributor
    t.top_revenue_industry,
    t.top_revenue_pct,
    t.top_revenue_alv,
    
    -- Anomaly detection
    an.anomaly_industry,
    an.anomaly_alv,
    an.anomaly_pct_above_typical,
    
    -- Goal comparison
    10000 as alv_target,
    round((s.avg_lot_value - 10000)::numeric, 2) as alv_variance,
    round((100.0 * (s.avg_lot_value - 10000) / 10000)::numeric, 2) as alv_variance_pct,
    case
        when not s.has_auctions then 'no_auction'
        when s.avg_lot_value >= 10000 then 'hit'
        else 'miss'
    end as goal_status

from weekly_summary s
left join industry_mix i on s.fiscal_year = i.fiscal_year and s.fiscal_week_number = i.fiscal_week_number
left join industry_alv a on s.fiscal_year = a.fiscal_year and s.fiscal_week_number = a.fiscal_week_number
left join region_mix r on s.fiscal_year = r.fiscal_year and s.fiscal_week_number = r.fiscal_week_number
left join region_alv ra on s.fiscal_year = ra.fiscal_year and s.fiscal_week_number = ra.fiscal_week_number
left join top_revenue t on s.fiscal_year = t.fiscal_year and s.fiscal_week_number = t.fiscal_week_number
left join anomalies an on s.fiscal_year = an.fiscal_year and s.fiscal_week_number = an.fiscal_week_number
order by s.fiscal_year, s.fiscal_week_number