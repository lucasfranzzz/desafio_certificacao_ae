with base_order_detail as
(
    select
        order_id
        , order_detail_id as detail_id
        , order_product_id as detail_product_id
        , order_detail_qty as detail_qty
        , order_special_offer_id as detail_special_offer_id
        , order_unit_price as detail_unit_price
        , order_unit_price_discount as detail_unit_price_discount
    from {{ ref('stg_sales_order_detail') }}
)

, base_order_header as
(
    select
        order_id
        , order_revision_number
        , order_date
        , order_due_date
        , order_ship_date
        , order_status
        , order_is_online
        , order_purchase_number
        , order_account_number
        , order_customer_id
        , order_sales_person_id
        , order_territory_id
        , order_bill_to_address_id
        , order_ship_to_address_id
        , order_ship_method_id
        , order_currency_rate_id
        , order_subtotal
        , order_tax_amt
        , order_freight
        , order_total_due
    from {{ ref('stg_sales_order_header') }}
)

, base_sales_order_header_sales_reason as
(
    select
        order_id
        , sales_reason_id
    from {{ ref('stg_sales_order_header_sales_reason') }}
)

, joined as 
( 
    select 
        base_order_detail.order_id
        , base_order_detail.detail_id

        , base_order_header.order_is_online
        , base_order_header.order_sales_person_id --dim_sales_person
        , base_order_header.order_customer_id --dim_customers
        , base_order_header.order_territory_id --?
        , base_order_header.order_bill_to_address_id --dim_address
        , base_order_header.order_ship_to_address_id --dim_address
        , base_order_header.order_ship_method_id --dim_ship_method
        , base_order_header.order_currency_rate_id --?
        , base_order_detail.detail_product_id --dim_product
        , base_order_detail.detail_special_offer_id --?
        , base_sales_order_header_sales_reason.sales_reason_id -- dim_sales_reason

        , base_order_header.order_revision_number
        , base_order_header.order_status
        , base_order_detail.detail_qty

        , base_order_detail.detail_unit_price
        , base_order_detail.detail_unit_price_discount
        , base_order_detail.detail_qty * base_order_detail.detail_unit_price as detail_value
        , detail_value * (1 - base_order_detail.detail_unit_price_discount) as detail_net_value

        , base_order_header.order_subtotal
        , round((detail_value / base_order_header.order_subtotal) * base_order_header.order_tax_amt,2) as detail_tax_amt
        , base_order_header.order_tax_amt
        , round((detail_value / base_order_header.order_subtotal) * base_order_header.order_freight,2) as detail_freight
        , base_order_header.order_freight
        , round((detail_value / base_order_header.order_subtotal) * base_order_header.order_total_due,2) as detail_total_due
        , base_order_header.order_total_due

        , base_order_header.order_date
        , base_order_header.order_due_date
        , base_order_header.order_ship_date
        
        , base_order_header.order_purchase_number
        , base_order_header.order_account_number
    from base_order_detail
    left join base_order_header
        on base_order_detail.order_id = base_order_header.order_id
    left join base_sales_order_header_sales_reason
        on base_order_header.order_id = base_sales_order_header_sales_reason.order_id
)

select * from joined where order_id = 74555