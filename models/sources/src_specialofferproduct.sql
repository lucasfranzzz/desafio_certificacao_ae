with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'specialofferproduct') }}
)

select * from raw