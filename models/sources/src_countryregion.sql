with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'countryregion') }}
)

select * from raw