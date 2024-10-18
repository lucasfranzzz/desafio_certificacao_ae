with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'product') }}
)

select * from raw