with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'productsubcategory') }}
)

select * from raw