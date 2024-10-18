with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'salesreason') }}
)

select * from raw