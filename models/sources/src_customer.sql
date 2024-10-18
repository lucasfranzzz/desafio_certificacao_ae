with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'customer') }}
)

select * from raw