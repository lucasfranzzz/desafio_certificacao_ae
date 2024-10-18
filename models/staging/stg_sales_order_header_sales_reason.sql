with source as 
(
    select
        cast(salesorderid as integer) as order_id
        , cast(salesreasonid as integer) as sales_reason_id
    from {{ ref('src_salesorderheaderssalesreason') }} 
)

select * from source