with base_country_region as
(
    select 
        country_region_code
        , country_region_name
    from {{ ref('stg_countryregion') }}
)

select * from base_country_region