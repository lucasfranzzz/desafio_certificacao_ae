with source as 
(
    select
        cast(shipmethodid as integer) as ship_method_id
        , cast(name as varchar) as ship_method_name
        , cast(shipbase as float) as ship_method_base
        , cast(shiprate as float) as ship_method_rate
    from {{ ref('src_shipmethod') }}
)

select * from source