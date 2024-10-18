with base_orders as
(
    select
        order_id
        , order_ship_to_address_id
        , detail_product_id
        , detail_value
        , detail_net_value
        , detail_discount_value
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

, base_product as
(
    select
        product_id
        , product_name
    from {{ ref('dim_product') }}
)

, joined as 
(
    select
        base_address.country_region_name
        , base_product.product_name
        , sum(base_orders.detail_value) as valor_negociado
        , sum(base_orders.detail_discount_value) as valor_desconto
        , sum(base_orders.detail_net_value) as valor_liquido_negociado
        , count(distinct base_orders.order_id) as qtd_pedidos
        , round((valor_liquido_negociado) / qtd_pedidos, 2) as ticket_medio
        , row_number() over (partition by base_address.country_region_name order by ticket_medio desc) as ticket_medio_rank
    from base_orders
    left join base_address
        on base_orders.order_ship_to_address_id = base_address.address_id
    left join base_product
        on base_product.product_id = base_orders.detail_product_id
    group by 1, 2
    order by ticket_medio desc
)

select * from joined where ticket_medio_rank <= 1
    order by ticket_medio desc