with source as 
(
    select
        cast(addressid as integer) as address_id
        , cast ((addressline1 || addressline2) as varchar) as address_line
        , cast (city as varchar) as address_city
        , cast (stateprovinceid as integer) as address_state_province_id
        , cast (postalcode as varchar) as address_postal_code
        , cast (spatiallocation as varchar) as address_spatial_location
    from {{ ref('src_address') }}
)

select * from source