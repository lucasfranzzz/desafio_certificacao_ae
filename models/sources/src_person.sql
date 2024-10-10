with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'person') }}
)

select * from raw