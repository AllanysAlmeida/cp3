WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_salespersonquotahistory') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY businessentityid, quotadate ORDER BY modifieddate DESC) AS row_num
    FROM source
),

renamed AS (
    SELECT
        coalesce(cast(businessentityid AS integer), 0) AS business_entity_id,
        coalesce(cast(quotadate AS date), date('1900-01-01')) AS quota_date,
        coalesce(cast(salesquota AS numeric(19, 4)), 0) AS sales_quota,
        coalesce(cast(modifieddate AS timestamp), timestamp('1900-01-01 00:00:00')) AS modified_at
    FROM deduplicated_source
    WHERE
        row_num = 1
)

SELECT * FROM renamed
WHERE business_entity_id != 0 AND quota_date != date('1900-01-01')
