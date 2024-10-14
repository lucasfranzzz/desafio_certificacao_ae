with base_orders as
(
    select
        order_date
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_value) as valor_total_negociado
        , sum(detail_qty) as quantidade_comprada
    from {{ ref('fct_orders') }}
    group by 1
)

, calendar as
(
    select
        date_day
        , month_start_date
        , year_start_date
    from {{ ref('dim_calendar') }}
)

, joined as 
(
    select
        year_start_date
        , sum(numero_de_pedidos) as numero_de_pedidos
        , sum(valor_total_negociado) as valor_total_negociado
        , sum(quantidade_comprada) as quantidade_comprada
    from calendar
    left join base_orders
        on calendar.date_day = base_orders.order_date
    group by 1
    order by 1
)

select * from joined