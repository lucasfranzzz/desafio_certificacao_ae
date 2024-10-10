with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'address') }}
)

select * from raw