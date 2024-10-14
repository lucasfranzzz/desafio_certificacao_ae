with source as 
(
    select
        cast(creditcardid as integer) as credit_card_id
        , cast(cardtype as varchar) as credit_card_type
        , cast(cardnumber as integer) as credit_card_number
    from {{ ref('src_creditcard') }}
)

select * from source