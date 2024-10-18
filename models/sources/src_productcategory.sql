with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'productcategory') }}
)

select * from raw