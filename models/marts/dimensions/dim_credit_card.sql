with base_credit_card as
(
    select 
        credit_card_id
        , credit_card_type
    from {{ ref('stg_credit_card') }}
)


select * from base_credit_card