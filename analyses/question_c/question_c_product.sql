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

, base_customer as
(
    select
        customer_id
        , customer_name
    from {{ ref('dim_customer') }}
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
        base_customer.customer_name
        , base_product.product_name
        , sum(base_orders.detail_value) as valor_total_negociado
        , row_number() over (partition by base_product.product_name order by valor_total_negociado desc, count(base_orders.order_id) desc) as valor_total_negociado_rank
    from base_orders
    left join base_customer
        on base_orders.order_customer_id = base_customer.customer_id
    left join base_product
        on base_orders.detail_product_id = base_product.product_id
    group by 1, 2
    order by 2 asc, valor_total_negociado_rank asc
)

select * from joined where valor_total_negociado_rank <= 10