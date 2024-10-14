with source as 
(
    select
        cast(currencycode as varchar) as currency_code
        , cast(name as varchar) as currency_name
    from {{ ref('src_currency') }} 
)

select * from source