with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'salesperson') }}
)

select * from raw