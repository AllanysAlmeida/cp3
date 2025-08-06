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
        trim(upper(countryregioncode)) AS country_region_code,
        initcap(trim(name)) AS country_region_name
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND trim(countryregioncode) IS NOT NULL
        AND trim(countryregioncode) != ''
        AND trim(name) IS NOT NULL
        AND trim(name) != ''
)

SELECT * FROM renamed
