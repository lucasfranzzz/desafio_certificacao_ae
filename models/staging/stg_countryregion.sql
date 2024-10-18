with source as 
(
    select
        cast(countryregioncode as varchar) as country_region_code
        , cast (name as varchar) as country_region_name
    from {{ ref('src_countryregion') }}
)

select * from source