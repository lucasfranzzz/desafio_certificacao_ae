with source as 
(
    select
        cast(currencyrateid as integer) as currency_rate_id
        , cast(currencyratedate as date) as currency_rate_date
        , cast(fromcurrencycode as varchar) as from_currency_code
        , cast(tocurrencycode as varchar) as to_currency_code
        , cast(averagerate as float) as currency_average_rate
        , cast(endofdayrate as float) as currency_end_of_day_rate
    from {{ ref('src_currencyrate') }} 
)

select * from source