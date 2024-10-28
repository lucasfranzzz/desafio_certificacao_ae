with base_orders as
(
    select
        detail_product_id
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_qty) as quantidade_comprada
        , sum(detail_value) as valor_total_negociado
    from {{ ref('fct_orders') }}
    group by detail_product_id
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
        base_product.product_name
        , base_orders.valor_total_negociado
        , base_orders.quantidade_comprada
        , base_orders.numero_de_pedidos
    from base_orders
    left join base_product
        on base_orders.detail_product_id = base_product.product_id
    order by 2 desc
)

select * from joined