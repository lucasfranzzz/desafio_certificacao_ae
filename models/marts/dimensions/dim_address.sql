with base_address as
(
    select
        address_id
        , address_line
        , address_city
        , address_state_province_id
    from {{ ref('stg_address') }}
)

, base_state_province as
(
    select 
        state_province_id
        , state_province_name
        , state_province_code
        , country_region_code
    from {{ ref('stg_state_province') }}
)

, base_country_region as
(
    select 
        country_region_code
        , country_region_name
    from {{ ref('stg_countryregion') }}
)

, joined as
(
    select
        base_address.address_id
        , base_address.address_line
        , base_address.address_city
        , base_state_province.state_province_name
        , base_state_province.state_province_code
        , base_country_region.country_region_name
        , base_state_province.country_region_code
    from base_address
    left join base_state_province
        on base_address.address_state_province_id = base_state_province.state_province_id
    left join base_country_region
        on base_state_province.country_region_code = base_country_region.country_region_code
)

select * from joined