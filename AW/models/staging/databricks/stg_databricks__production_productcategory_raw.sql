WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'production_productcategory') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY productcategoryid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE productcategoryid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(productcategoryid AS integer), 0) AS product_category_id,
        coalesce(trim(name), 'Unknown') AS category_name
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(productcategoryid, 0) > 0
        AND coalesce(trim(name), 'Unknown') != ''
)

SELECT * FROM renamed
