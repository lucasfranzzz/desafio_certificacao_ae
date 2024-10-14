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

, base_reason as
(
    select
        order_id
        , ifnull(sales_reason_name, 'no_reason') as sales_reason_name
    from {{ ref('dim_order_sales_reason') }}
)

, joined as 
(
    select
        base_reason.sales_reason_name
        , base_address.address_city
        , sum(base_orders.detail_value) as valor_total_negociado
        , row_number() over (partition by base_reason.sales_reason_name order by valor_total_negociado desc, count(base_orders.order_id) desc) as valor_total_negociado_rank
    from base_orders
    left join base_address
        on base_orders.order_ship_to_address_id = base_address.address_id
    left join base_reason
        on base_orders.order_id = base_reason.order_id
    group by 1, 2
    order by 1 asc, valor_total_negociado_rank asc
)

select * from joined where valor_total_negociado_rank <= 5