with base_currency_rate as 
(
    select
        *
    from {{ ref('stg_currency_rate') }}
)

, base_currency as 
(
    select
        *
    from {{ ref('stg_currency') }}
)

, joined as
(
    select
        base_currency_rate.currency_rate_id
        , base_currency_rate.currency_rate_date
        , from_base_currency.currency_name as from_currency_name
        , to_base_currency.currency_name as to_currency_name
        , base_currency_rate.currency_average_rate
        , base_currency_rate.currency_end_of_day_rate
    from base_currency_rate
    left join base_currency as from_base_currency
        on base_currency_rate.from_currency_code = from_base_currency.currency_code
    left join base_currency as to_base_currency
        on base_currency_rate.to_currency_code = to_base_currency.currency_code
)

select * from joined