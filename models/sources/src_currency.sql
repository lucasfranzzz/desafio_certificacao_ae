with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'currency') }}
)

select * from raw