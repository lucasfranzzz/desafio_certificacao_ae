with base_product as
(
    select
        product_id
        , product_name
        , product_number
        , product_make_flag
        , product_finished_goods_flag
        , product_color
        , product_safety_stock_level
        , product_reorder_point
        , product_standard_cost
        , product_list_price
        , product_days_to_manufacture
        , product_line
        , product_class
        , product_style
        , product_subcategory_id
        , product_model_id
        , product_sell_start_date
        , product_sell_end_date
        , product_discontinued_date
    from {{ ref('stg_product') }}
)

, base_subcategory as 
(
    select
        product_subcategory_id
        , product_category_id
        , product_subcategory_name
    from {{ ref('stg_product_subcategory') }}
)

, base_category as 
(
    select
        product_category_id
        , product_category_name
    from {{ ref('stg_product_category') }}
)

, subcategory_category as 
(
    select
        base_subcategory.product_subcategory_id
        , base_subcategory.product_subcategory_name
        , base_category.product_category_name
    from base_subcategory
    left join base_category
        on base_subcategory.product_category_id = base_category.product_category_id
)

, joined as
(
    select
        base_product.product_id
        , base_product.product_name
        , base_product.product_number
        , base_product.product_make_flag
        , base_product.product_finished_goods_flag
        , base_product.product_color
        , base_product.product_safety_stock_level
        , base_product.product_reorder_point
        , base_product.product_standard_cost
        , base_product.product_list_price
        , base_product.product_days_to_manufacture
        , base_product.product_line
        , base_product.product_class
        , base_product.product_style
        , subcategory_category.product_subcategory_name
        , subcategory_category.product_category_name
        , base_product.product_model_id
        , base_product.product_sell_start_date
        , base_product.product_sell_end_date
        , base_product.product_discontinued_date
    from base_product
    left join subcategory_category
        on base_product.product_subcategory_id = subcategory_category.product_subcategory_id
)

select * from joined