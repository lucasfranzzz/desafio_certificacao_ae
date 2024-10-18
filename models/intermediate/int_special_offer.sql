with base_special_offer as 
(
    select
        special_offer_id
        , special_offer_description
        , special_offer_discount_pct
        , special_offer_type
        , special_offer_category
        , special_offer_start_date
        , special_offer_end_date
        , special_offer_min_qty
        , special_offer_max_qty
    from {{ ref('stg_special_offer') }}
)

, base_special_offer_product as
(
    select
        special_offer_id
        , special_offer_product_id
    from {{ ref('stg_special_offer_product') }}
)

, joined as 
(
    select
        base_special_offer_product.special_offer_id
        , base_special_offer_product.special_offer_product_id
        , base_special_offer.special_offer_description
        , base_special_offer.special_offer_discount_pct
        , base_special_offer.special_offer_type
        , base_special_offer.special_offer_category
        , base_special_offer.special_offer_start_date
        , base_special_offer.special_offer_end_date
        , base_special_offer.special_offer_min_qty
        , base_special_offer.special_offer_max_qty
    from base_special_offer_product
    left join base_special_offer
        on base_special_offer_product.special_offer_id = base_special_offer.special_offer_id
)

select * from joined