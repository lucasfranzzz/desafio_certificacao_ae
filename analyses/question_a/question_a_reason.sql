with base_orders as
(
    select
        order_id
        , detail_qty
        , detail_value
    from {{ ref('fct_orders') }}
)

, base_reason as
(
    select
        order_id
        , sales_reason_name
    from {{ ref('dim_order_sales_reason') }}
)

, joined as
(
    select
        ifnull(base_reason.sales_reason_name, 'no_reason') as sales_reason_name
        , count(distinct base_orders.order_id) as numero_de_pedidos
        , sum(base_orders.detail_qty) as quantidade_comprada
        , sum(base_orders.detail_value) as valor_total_negociado
    from base_orders
    left join base_reason
        on base_orders.order_id = base_reason.order_id
    group by 1
    order by 1
)

select * from joined