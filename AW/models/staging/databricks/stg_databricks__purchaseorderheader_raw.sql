WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'purchaseorderheader') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY purchaseorderid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE purchaseorderid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(purchaseorderid AS integer), 0) AS purchase_order_id,
        coalesce(cast(orderdate AS date), date('1900-01-01')) AS order_date
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(purchaseorderid, 0) > 0
        AND coalesce(orderdate, date('1900-01-01')) >= date('2005-01-01')
        AND coalesce(orderdate, date('1900-01-01')) <= current_date()
)

SELECT * FROM renamed
