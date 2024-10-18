with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'stateprovince') }}
)

select * from raw