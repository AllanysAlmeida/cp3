WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_customer') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY customerid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE customerid IS NOT null
),

renamed AS (
    SELECT
        cast(customerid AS integer) AS customer_id,
        cast(personid AS integer) AS person_id,
        cast(storeid AS integer) AS store_id,
        cast(territoryid AS integer) AS territory_id,
        CASE 
            WHEN storeid IS NOT NULL THEN 'B2B'
            ELSE 'B2C'
        END AS customer_type
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND customerid IS NOT NULL
)

SELECT * FROM renamed