with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'employee') }}
)

select * from raw