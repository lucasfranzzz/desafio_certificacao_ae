with base_person as
(
    select
        person_id
        , person_type
        , title
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

, store_person as
(
    select
        base_store.store_id
        , base_store.store_name
        , base_person.name as store_sales_person_name
    from base_store
    left join base_person
        on base_store.store_sales_person_id = base_person.person_id
)

, base_state_province as
(
    select
        state_province_id
        , state_province_name
        , state_province_territory_id
        , state_province_code
        , country_region_code
    from {{ ref('stg_state_province') }}
)

, joined as
(
    select
        base_customer.customer_id
        , base_person.person_id as customer_person_id
        , base_person.title as customer_title
        , base_person.name as customer_name
        , base_customer.customer_territory_id
        , store_person.store_id as customer_store_id
        , store_person.store_name as customer_store_name
        , store_person.store_sales_person_name as store_sales_person_name
        , base_state_province.*
    from base_customer
    left join base_person
        on base_customer.customer_person_id = base_person.person_id
    left join store_person
        on base_customer.customer_store_id = store_person.store_id
    left join base_state_province
        on base_customer.customer_territory_id = base_state_province.state_province_territory_id
)

select * from joined