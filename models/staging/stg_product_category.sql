with source as 
(
    select
        cast(productcategoryid as integer) as product_category_id
        , cast(name as varchar) as product_category_name
    from {{ ref('src_productcategory') }} 
)

select * from source