with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'creditcard') }}
)

select * from raw