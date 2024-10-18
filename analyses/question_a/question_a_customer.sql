with base_orders as
(
    select
        order_customer_id
        , sum(detail_value) as valor_total_negociado
        , sum(detail_qty) as quantidade_comprada
        , count(distinct order_id) as numero_de_pedidos
    from {{ ref('fct_orders') }}
    group by order_customer_id
    order by 2 desc
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
        , base_orders.valor_total_negociado
        , base_orders.quantidade_comprada
        , base_orders.numero_de_pedidos
    from base_orders
    left join base_customer
        on base_orders.order_customer_id = base_customer.customer_id
    order by 2 desc

)

select * from joined