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

, base_customer as
(
    select
        customer_id
        , customer_name
    from {{ ref('dim_customer') }}
)

, base_credit_card as
(
    select
        credit_card_id
        , ifnull(credit_card_type, 'no_credit_card') as credit_card_type
    from {{ ref('dim_credit_card') }}
)

, joined as 
(
    select
        base_customer.customer_name
        , base_credit_card.credit_card_type
        , sum(base_orders.detail_value) as valor_total_negociado
        , row_number() over (partition by base_credit_card.credit_card_type order by valor_total_negociado desc, count(base_orders.order_id) desc) as valor_total_negociado_rank
    from base_orders
    left join base_customer
        on base_orders.order_customer_id = base_customer.customer_id
    left join base_credit_card
        on base_orders.order_credit_card_id = base_credit_card.credit_card_id
    group by 1, 2
    order by 2 asc, valor_total_negociado_rank asc
)

select * from joined where valor_total_negociado_rank <= 10