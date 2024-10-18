with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'shipmethod') }}
)

select * from raw