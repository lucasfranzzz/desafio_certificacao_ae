with base_person as
(
    select
        person_id
        , person_type
        , name
        , email_promotion
    from {{ ref('stg_person') }}
)

, base_customer as
(
    select 
        customer_id
        , customer_person_id
        , customer_store_id
        , customer_territory_id
    from {{ ref('stg_customer') }}
)

, base_store as
(
    select 
        store_id
        , store_name
        , store_sales_person_id
    from {{ ref('stg_store') }}
)

, joined as
(
    select
        base_customer.customer_id
        , base_person.person_id as customer_person_id
        , ifnull(base_person.name,'Name not provided') as customer_name
        , base_customer.customer_territory_id
    from base_customer
    left join base_person
        on base_customer.customer_person_id = base_person.person_id
    left join base_store
        on base_customer.customer_store_id = base_store.store_id
    order by base_customer.customer_id
)

select * from joined