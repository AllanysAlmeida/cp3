WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_salesorderheadersalesreason') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY salesorderid, salesreasonid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE salesorderid IS NOT null AND salesreasonid IS NOT null
),

renamed AS (
    SELECT
        cast(salesorderid AS integer) AS sales_order_id,
        cast(salesreasonid AS integer) AS sales_reason_id
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND salesorderid IS NOT NULL
        AND salesreasonid IS NOT NULL
)

SELECT * FROM renamed