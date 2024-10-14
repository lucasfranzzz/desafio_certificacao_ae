with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'specialoffer') }}
)

select * from raw