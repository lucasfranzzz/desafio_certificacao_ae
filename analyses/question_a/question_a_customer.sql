with base_orders as
(
    select
        order_customer_id
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_qty) as quantidade_comprada
        , sum(detail_value) as valor_total_negociado
    from {{ ref('fct_orders') }}
    group by order_customer_id
    order by 1
)

, base_customer as
(
    select
        customer_id
        , customer_name
    from {{ ref('dim_customer') }}
)

, joined as
(
    select
        base_customer.customer_name
        , base_orders.numero_de_pedidos
        , base_orders.quantidade_comprada
        , base_orders.valor_total_negociado
    from base_orders
    left join base_customer
        on base_orders.order_customer_id = base_customer.customer_id

)

select * from joined