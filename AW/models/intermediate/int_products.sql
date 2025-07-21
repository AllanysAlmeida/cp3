-- int_products: Dimensão de produtos enriquecida
-- Foco: Integrar produtos com categorias e subcategorias
-- Materialização: table para consultas frequentes

WITH stg_products AS (
    SELECT * FROM {{ ref('stg_databricks__production_product_raw') }}
),

stg_subcategories AS (
    SELECT * FROM {{ ref('stg_databricks__production_productsubcategory_raw') }}
),

stg_categories AS (
    SELECT * FROM {{ ref('stg_databricks__production_productcategory_raw') }}
)

SELECT
    p.product_id,
    p.product_subcategory_id,
    p.standard_cost,
    p.list_price,
    s.product_category_id,
    s.subcategory_name,
    c.category_name
FROM stg_products AS p
LEFT JOIN stg_subcategories AS s ON p.product_subcategory_id = s.product_subcategory_id
LEFT JOIN stg_categories AS c ON s.product_category_id = c.product_category_id
