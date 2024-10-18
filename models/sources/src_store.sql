with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'store') }}
)

select * from raw