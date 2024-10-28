with base_int_orders_detailed as
(
    select
        *
    from {{ ref('int_orders_detailed') }}
)

, base_currency_rate as 
(
    select
        currency_rate_id
        , currency_average_rate
    from {{ ref('int_currency_rate') }}
)

, base_special_offer as 
(
    select
        special_offer_id
        , special_offer_product_id
        , special_offer_description
    from {{ ref('int_special_offer') }}
)

, joined as 
( 
    select 
        md5(concat(
            coalesce(base_int_orders_detailed.order_id,'_this_used_to_be_null_'),
            coalesce(base_int_orders_detailed.detail_id,'_this_used_to_be_null_'))
            ) as sk_order_detail
        , base_int_orders_detailed.order_id
        , base_int_orders_detailed.detail_id

        , base_int_orders_detailed.order_sales_person_id --dim_sales_person
        , base_int_orders_detailed.order_customer_id --dim_customers
        , base_int_orders_detailed.order_ship_to_address_id --dim_address
        , base_int_orders_detailed.detail_product_id --dim_product
        , base_int_orders_detailed.order_credit_card_id --dim_credit_card
        , base_int_orders_detailed.order_date --dim_calendar

        , base_currency_rate.currency_average_rate
        , base_int_orders_detailed.order_revision_number
        , base_int_orders_detailed.order_status
        , base_int_orders_detailed.detail_qty

        , base_int_orders_detailed.detail_unit_price
        , base_int_orders_detailed.detail_value
        , base_int_orders_detailed.detail_unit_price_discount_pct
        , base_int_orders_detailed.detail_discount_value
        , base_int_orders_detailed.detail_net_value

        , base_int_orders_detailed.order_subtotal
        , base_int_orders_detailed.detail_tax_amt
        , base_int_orders_detailed.order_tax_amt
        , base_int_orders_detailed.detail_freight
        , base_int_orders_detailed.order_freight
        , base_int_orders_detailed.detail_total_due
        , base_int_orders_detailed.order_total_due

        , base_int_orders_detailed.order_due_date
        , base_int_orders_detailed.order_ship_date
        
        , base_int_orders_detailed.order_purchase_number
        , base_int_orders_detailed.order_account_number
        , base_int_orders_detailed.order_method
        , base_special_offer.special_offer_description

    from base_int_orders_detailed
    left join base_currency_rate
        on base_int_orders_detailed.order_currency_rate_id = base_currency_rate.currency_rate_id
    left join base_special_offer
        on base_int_orders_detailed.detail_special_offer_id = base_special_offer.special_offer_id
        and base_int_orders_detailed.detail_product_id = base_special_offer.special_offer_product_id
)

select * from joined