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
        coalesce(cast(salesreasonid AS integer), 0) AS sales_reason_id,
        coalesce(trim(name), 'Unknown') AS sales_reason_name,
        coalesce(trim(upper(reasontype)), 'Unknown') AS reason_type
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(salesreasonid, 0) > 0
        AND coalesce(trim(name), 'Unknown') != ''
)

SELECT * FROM renamed
