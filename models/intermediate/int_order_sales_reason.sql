with base_sales_reason as
(
    select
        sales_reason_id
        , sales_reason_name
        , sales_reason_type
    from {{ ref('stg_sales_reason') }}
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
        base_sales_order_header_sales_reason.order_id
        , base_sales_order_header_sales_reason.sales_reason_id
        , base_sales_reason.sales_reason_name
        , base_sales_reason.sales_reason_type
    from base_sales_order_header_sales_reason
    left join base_sales_reason
        on base_sales_order_header_sales_reason.sales_reason_id = base_sales_reason.sales_reason_id
)

select * from joined