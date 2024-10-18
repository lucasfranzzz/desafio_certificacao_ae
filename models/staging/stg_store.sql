with source as 
(
    select
        cast(businessentityid as integer) as store_id
        , cast(name as varchar) as store_name
        , cast(salespersonid as integer) as store_sales_person_id
    from {{ ref('src_store') }} 
)

select * from source