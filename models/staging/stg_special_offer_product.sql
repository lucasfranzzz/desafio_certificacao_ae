with source as 
(
    select
        cast(specialofferid as integer) as special_offer_id
        , cast (productid as integer) as special_offer_product_id
    from {{ ref('src_specialofferproduct') }}
)

select * from source