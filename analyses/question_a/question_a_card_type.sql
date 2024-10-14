with base_orders as
(
    select
        order_credit_card_id
        , order_id
        , detail_qty
        , detail_value
    from {{ ref('fct_orders') }}
)

, base_credit_card as
(
    select
        credit_card_id
        , credit_card_type
    from {{ ref('dim_credit_card') }}
)

, joined as
(
    select
        ifnull(base_credit_card.credit_card_type, 'no_credit_card') as credit_card_type
        , count(distinct order_id) as numero_de_pedidos
        , sum(detail_qty) as quantidade_comprada
        , sum(detail_value) as valor_total_negociado
    from base_orders
    left join base_credit_card
        on base_orders.order_credit_card_id = base_credit_card.credit_card_id
    group by credit_card_type 
    order by 2 desc, 3 desc, 4 desc
)

select * from joined