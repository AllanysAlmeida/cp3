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
        cast(productid AS integer) AS product_id,
        trim(name) AS product_name,
        cast(productsubcategoryid AS integer) AS product_subcategory_id,
        coalesce(cast(listprice AS numeric(19, 4)), 0) AS list_price
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND productid IS NOT NULL
        AND trim(name) IS NOT NULL
        AND trim(name) != ''
)

SELECT * FROM renamed
