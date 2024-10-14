with base_orders as
(
    select
        order_date
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_qty) as quantidade_comprada
        , sum(detail_value) as valor_total_negociado
    from {{ ref('fct_orders') }}
    group by order_date
    order by order_date
)

select * from base_orders