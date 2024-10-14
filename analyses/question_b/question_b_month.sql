with base_orders as
(
    select
        order_date
        , detail_product_id
        , order_id
        , detail_net_value
    from {{ ref('fct_orders') }}
)

, calendar as
(
    select
        date_day
        , month_start_date
    from {{ ref('dim_calendar') }}
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
        calendar.month_start_date
        , base_product.product_name
        , sum(base_orders.detail_net_value) as valor_liquido_negociado
        , count(distinct base_orders.order_id) as qtd_pedidos
        , round(valor_liquido_negociado / qtd_pedidos, 2) as ticket_medio
        , row_number() over (partition by calendar.month_start_date order by ticket_medio desc) as ticket_medio_rank
    from base_orders
    left join calendar
        on calendar.date_day = base_orders.order_date
    left join base_product
        on base_product.product_id = base_orders.detail_product_id
    group by 1, 2
    order by 1 asc, ticket_medio_rank asc
)

select * from joined where ticket_medio_rank <= 5