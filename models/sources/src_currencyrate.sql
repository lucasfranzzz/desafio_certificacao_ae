with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'currencyrate') }}
)

select * from raw