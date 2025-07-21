WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'person_countryregion') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY countryregioncode ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE countryregioncode IS NOT null
),

renamed AS (
    SELECT
        coalesce(trim(upper(countryregioncode)), 'Unknown') AS country_region_code,
        coalesce(trim(name), 'Unknown') AS country_region_name
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(trim(countryregioncode), 'Unknown') != ''
        AND coalesce(trim(name), 'Unknown') != ''
)

SELECT * FROM renamed
