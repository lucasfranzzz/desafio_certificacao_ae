with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'salesorderdetail') }}
)

select * from raw