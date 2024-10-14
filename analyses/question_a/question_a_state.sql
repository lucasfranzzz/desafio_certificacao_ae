with base_orders as
(
    select
        order_ship_to_address_id
        , order_id
        , detail_qty
        , detail_value
    from {{ ref('fct_orders') }}
)

, base_address as
(
    select
        address_id
        , address_city
        , state_province_name
        , country_region_name
    from {{ ref('dim_address') }}
)

, joined as
(
    select
        base_address.state_province_name
        , count(distinct base_orders.order_id) as numero_de_pedidos
        , sum(base_orders.detail_qty) as quantidade_comprada
        , sum(base_orders.detail_value) as valor_total_negociado
    from base_orders
    left join base_address
        on base_orders.order_ship_to_address_id = base_address.address_id
    group by 1
    order by 1
)

select * from joined