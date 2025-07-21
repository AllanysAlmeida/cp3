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
        coalesce(cast(salesorderid AS integer), 0) AS sales_order_id,
        coalesce(cast(salesreasonid AS integer), 0) AS sales_reason_id
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(salesorderid, 0) > 0
        AND coalesce(salesreasonid, 0) > 0
)

SELECT * FROM renamed