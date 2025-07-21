WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_salesterritory') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY territoryid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE territoryid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(territoryid AS integer), 0) AS territory_id,
        coalesce(trim(name), 'Unknown') AS territory_name,
        coalesce(trim(upper(countryregioncode)), 'Unknown') AS country_region_code
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(territoryid, 0) > 0
        AND coalesce(trim(name), 'Unknown') != ''
        AND coalesce(trim(countryregioncode), 'Unknown') != ''
        AND length(coalesce(trim(countryregioncode), 'Unknown')) <= 3
)

SELECT * FROM renamed
