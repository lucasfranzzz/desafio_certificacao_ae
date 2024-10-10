with base_ship_method as
(
    select 
        ship_method_id
        , ship_method_name
        , ship_method_base
        , ship_method_rate
    from {{ ref('stg_ship_method') }}
)

select * from base_ship_method