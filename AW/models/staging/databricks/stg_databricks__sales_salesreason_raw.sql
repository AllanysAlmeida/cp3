WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_salesreason') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY salesreasonid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE salesreasonid IS NOT null
),

renamed AS (
    SELECT
        cast(salesreasonid AS integer) AS sales_reason_id,
        trim(name) AS reason_name,
        trim(upper(reasontype)) AS reason_type
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND salesreasonid IS NOT NULL
        AND trim(name) IS NOT NULL
        AND trim(name) != ''
)

SELECT * FROM renamed
