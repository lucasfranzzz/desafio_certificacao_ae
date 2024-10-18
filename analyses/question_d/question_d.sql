with base_orders as
(
    select
        order_customer_id
        , detail_value
        , detail_product_id
        , order_credit_card_id
        , order_id
        , order_date
        , order_status
        , order_ship_to_address_id
    from {{ ref('fct_orders') }}
)

, base_address as
(
    select
        address_id
        , address_city
    from {{ ref('dim_address') }}
)

, joined as 
(
    select
        base_address.address_city
        , sum(base_orders.detail_value) as valor_total_negociado
        , row_number() over (order by valor_total_negociado desc) as valor_total_negociado_rank
    from base_orders
    left join base_address
        on base_orders.order_ship_to_address_id = base_address.address_id
    group by 1
    order by 2 desc
)

select * from joined where valor_total_negociado_rank <= 5