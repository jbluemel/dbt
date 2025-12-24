{{ config(materialized='table')}}

with fee_pivot as (
    select
        item_id,
        sum(case when fee_type= 'Seller Service Fee' then fee_amount else 0 end) as seller_service_fee,
        sum(case when fee_type= 'Lot Fee' then fee_amount else 0 end) as lot_fee,
        sum(case when fee_type='Power Washing' then fee_amount else 0 end) as power_washing,
        sum(case when fee_type='Decal Removal' then fee_amount else 0 end) as decal_removal,
        sum(fee_amount) as total_fees
    from {{source('raw_data', 'fees')}}
    group by item_id

)

select
    i.unique_id,
    i.auctiondate,
    i.icn,
    i.model,
    i.category,
    i.hammer,
    i.contract_price,
    coalesce(f.seller_service_fee, 0) as seller_service_fee,
    coalesce(f.lot_fee, 0) as lot_fee,
    coalesce(f.power_washing, 0) as power_washing,
    coalesce(f.decal_removal, 0) as decal_removal,
    coalesce(f.total_fees, 0) as total_fees
from {{ source('raw_data', 'items_v2')}} i
left join fee_pivot f on i.unique_id = f.item_id