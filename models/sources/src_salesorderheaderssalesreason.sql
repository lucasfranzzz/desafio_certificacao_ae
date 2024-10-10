with raw as 
(
    select 
        *
    from {{ source('raw_adventure_works', 'salesorderheadersalesreason') }}
)

select * from raw