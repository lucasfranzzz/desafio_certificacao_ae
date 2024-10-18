with base_orders as
(
    select
        order_status
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_qty) as quantidade_comprada
        , sum(detail_value) as valor_total_negociado
    from {{ ref('fct_orders') }}
    group by order_status
    order by 1
)

select * from base_orders