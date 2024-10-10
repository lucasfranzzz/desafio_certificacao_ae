with source as 
(
    select
        cast(salesreasonid as integer) as sales_reason_id
        , cast(name as varchar) as sales_reason_name
        , cast(reasontype as varchar) as sales_reason_type
    from {{ ref('src_salesreason') }} 
)

select * from source