with base_sales_reason as
(
    select
        sales_reason_id
        , sales_reason_name
        , sales_reason_type
    from {{ ref('stg_sales_reason') }}
)

select * from base_sales_reason