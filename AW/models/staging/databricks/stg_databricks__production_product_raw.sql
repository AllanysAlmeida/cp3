WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'production_product') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY productid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE productid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(productid AS integer), 0) AS product_id,
        coalesce(cast(productsubcategoryid AS integer), 0) AS product_subcategory_id,
        coalesce(cast(standardcost AS numeric(19, 4)), 0) AS standard_cost,
        coalesce(cast(listprice AS numeric(19, 4)), 0) AS list_price
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(productid, 0) > 0
        AND coalesce(standardcost, 0) >= 0
        AND coalesce(listprice, 0) >= 0
)

SELECT * FROM renamed
