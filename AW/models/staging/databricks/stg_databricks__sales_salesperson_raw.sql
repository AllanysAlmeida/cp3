WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_salesperson') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY businessentityid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE businessentityid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(businessentityid AS integer), 0) AS business_entity_id,
        coalesce(cast(commissionpct AS numeric(19, 4)), 0) AS commission_pct,
        coalesce(cast(salesquota AS numeric(19, 4)), 0) AS sales_quota
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(businessentityid, 0) > 0
        AND coalesce(commissionpct, 0) >= 0
        AND coalesce(commissionpct, 0) <= 1
        AND coalesce(salesquota, 0) >= 0
)

SELECT * FROM renamed
