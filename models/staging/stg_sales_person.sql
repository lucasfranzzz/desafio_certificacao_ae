with source as 
(
    select
        cast(businessentityid as integer) as sales_person_id
        , cast(territoryid as integer) as territory_id
        , cast(salesquota as number(38,2)) as sales_quota
        , cast(bonus as number(38,2)) as bonus
        , cast(commissionpct as float) as commission_pct
        , cast(salesytd as number(38,2)) as sales_ytd
        , cast(saleslastyear as number(38,2)) as sales_last_year
    from {{ ref('src_salesperson') }} 
)

select * from source