with source as 
(
    select
        cast(productsubcategoryid as integer) as product_subcategory_id
        , cast(productcategoryid as integer) as product_category_id
        , cast(name as varchar) as product_subcategory_name
    from {{ ref('src_productsubcategory') }} 
)

select * from source