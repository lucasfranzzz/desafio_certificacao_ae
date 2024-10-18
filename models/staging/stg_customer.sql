with source as 
(
    select
        cast(customerid as integer) as customer_id
        , cast (personid as varchar) as customer_person_id
        , cast (storeid as integer) as customer_store_id
        , cast (territoryid as varchar) as customer_territory_id
    from {{ ref('src_customer') }}
)

select * from source