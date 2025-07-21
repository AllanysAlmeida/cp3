WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'production_productcosthistory') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY productid, startdate ORDER BY modifieddate DESC) AS row_num
    FROM source
),

renamed AS (
    SELECT
        coalesce(cast(productid AS integer), 0) AS product_id,
        coalesce(cast(startdate AS date), date('1900-01-01')) AS start_date,
        coalesce(cast(enddate AS date), date('1900-01-01')) AS end_date,
        coalesce(cast(standardcost AS numeric(19, 4)), 0) AS standard_cost,
        coalesce(cast(modifieddate AS timestamp), timestamp('1900-01-01 00:00:00')) AS modified_at
    FROM deduplicated_source
    WHERE
        row_num = 1
)

SELECT * FROM renamed
WHERE product_id != 0 AND start_date != date('1900-01-01')
