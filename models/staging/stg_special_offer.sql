with source as 
(
    select
        cast(specialofferid as integer) as special_offer_id
        , cast(description as varchar) as special_offer_description
        , cast(discountpct as float) as special_offer_discount_pct
        , cast(type as varchar) as special_offer_type
        , cast(category as varchar) as special_offer_category
        , cast(startdate as date) as special_offer_start_date
        , cast(enddate as date) as special_offer_end_date
        , cast(minqty as number(38,2)) as special_offer_min_qty
        , cast(maxqty as number(38,2)) as special_offer_max_qty
    from {{ ref('src_specialoffer') }}
)

select * from source