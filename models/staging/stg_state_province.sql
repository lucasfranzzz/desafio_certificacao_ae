with source as 
(
    select
        cast(stateprovinceid as integer) as state_province_id
        , cast(name as varchar) as state_province_name
        , cast(territoryid as integer) as state_province_territory_id
        , cast(stateprovincecode as varchar) as state_province_code
        , cast(countryregioncode as varchar) as country_region_code
        , cast(isonlystateprovinceflag as boolean) as is_only_state_province
    from {{ ref('src_stateprovince') }} 
)

select * from source