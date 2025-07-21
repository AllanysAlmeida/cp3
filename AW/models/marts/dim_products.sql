-- dim_products: Dimensão de produtos para BI
-- Modelagem dimensional star schema com hierarquia de categorias

SELECT
    product_id,
    product_subcategory_id,
    product_category_id,
    subcategory_name,
    category_name,
    standard_cost,
    list_price
FROM {{ ref('int_products') }}
