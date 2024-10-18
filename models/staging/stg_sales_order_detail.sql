with source as 
(
    select
        cast(salesorderid as integer) as order_id
        , cast(salesorderdetailid as integer) as order_detail_id
        , cast(orderqty as number(18,2)) as order_detail_qty
        , cast(productid as integer) as order_product_id
        , cast(specialofferid as integer) as order_special_offer_id
        , cast(unitprice as float) as order_unit_price
        , cast(unitpricediscount as float) as order_unit_price_discount
    from {{ ref('src_salesorderdetail') }} 
)

select * from source