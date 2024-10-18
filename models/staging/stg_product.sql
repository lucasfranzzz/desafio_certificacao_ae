with source as 
(
    select
        cast(productid as int) as product_id
        , cast(name as varchar) as product_name
        , cast(productnumber as varchar) as product_number
        , cast(makeflag as boolean) as product_make_flag
        , cast(finishedgoodsflag as boolean) as product_finished_goods_flag
        , cast(color as varchar) as product_color
        , cast(safetystocklevel as number(38,2)) as product_safety_stock_level
        , cast(reorderpoint as number(38,2)) as product_reorder_point
        , cast(standardcost as number(38,2)) as product_standard_cost
        , cast(listprice as number(38,2)) as product_list_price
        , cast(daystomanufacture as number(38,2)) as product_days_to_manufacture
        , cast(ifnull(productline,'Line not provided') as varchar) as product_line
        , cast(ifnull(class,'Class not provided') as varchar) as product_class
        , cast(ifnull(style,'Style not provided') as varchar) as product_style
        , cast(productsubcategoryid as varchar) as product_subcategory_id
        , cast(productmodelid as varchar) as product_model_id
        , cast(sellstartdate as date) as product_sell_start_date
        , cast(sellenddate as date) as product_sell_end_date
        , discontinueddate as product_discontinued_date
    from {{ ref('src_product') }} 
)

select * from source