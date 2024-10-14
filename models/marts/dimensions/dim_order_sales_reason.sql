with base_int_order_sales_reason as
(
    select
        md5(concat(
            coalesce(order_id,'_this_used_to_be_null_'),
            coalesce(sales_reason_id,'_this_used_to_be_null_'))
            ) as sk_order_sales_reason
        , order_id
        , sales_reason_id
        , sales_reason_name
        , sales_reason_type
    from {{ ref('int_order_sales_reason') }}
)

select * from base_int_order_sales_reason