with source as 
(
    select
        cast(salesorderid as integer) as order_id
        , cast(revisionnumber as integer) as order_revision_number
        , cast(orderdate as date) as order_date
        , cast(duedate as date) as order_due_date
        , cast(shipdate as date) as order_ship_date
        , cast(status as integer) as order_status
        , cast(onlineorderflag as boolean) as order_is_online
        , cast(creditcardid as integer) as order_credit_card_id
        , cast(purchaseordernumber as varchar) as order_purchase_number
        , cast(accountnumber as varchar) as order_account_number
        , cast(customerid as integer) as order_customer_id
        , cast(salespersonid as integer) as order_sales_person_id
        , cast(territoryid as integer) as order_territory_id
        , cast(billtoaddressid as integer) as order_bill_to_address_id
        , cast(shiptoaddressid as integer) as order_ship_to_address_id
        , cast(shipmethodid as integer) as order_ship_method_id
        , cast(currencyrateid as integer) as order_currency_rate_id
        , cast(subtotal as float) as order_subtotal
        , cast(taxamt as float) as order_tax_amt
        , cast(freight as float) as order_freight
        , cast(totaldue as float) as order_total_due
    from {{ ref('src_salesorderheader') }} 
)

select * from source