with base_orders as
(
    select
        order_id
        , detail_product_id
        , detail_qty
    from {{ ref('fct_orders') }}
)

, base_product as
(
    select
        product_id
        , product_name
    from {{ ref('dim_product') }}
)

, base_reason as
(
    select
        order_id
        , sales_reason_type
    from {{ ref('dim_order_sales_reason') }}
)

, joined as
(
    select
        product_id
        , product_name
        , sum(detail_qty) as quantidade_comprada
    from base_orders
    left join base_product
        on base_orders.detail_product_id = base_product.product_id
    left join base_reason
        on base_orders.order_id = base_reason.order_id
        and base_reason.sales_reason_type = 'Promotion'
    group by 1, 2
    order by 3 desc
    limit 10
)

select * from joined