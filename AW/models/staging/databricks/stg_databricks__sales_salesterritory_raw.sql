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
        cast(territoryid AS integer) AS territory_id,
        trim(name) AS territory_name,
        trim(upper(countryregioncode)) AS country_region_code,
        trim(upper(group)) AS territory_group
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND territoryid IS NOT NULL
        AND trim(name) IS NOT NULL
        AND trim(name) != ''
        AND trim(countryregioncode) IS NOT NULL
        AND trim(countryregioncode) != ''
        AND length(trim(countryregioncode)) <= 3
)

SELECT * FROM renamed
