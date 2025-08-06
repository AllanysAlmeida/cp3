WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'person_stateprovince') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY stateprovinceid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE stateprovinceid IS NOT null
),

renamed AS (
    SELECT
        cast(stateprovinceid AS integer) AS state_province_id,
        trim(upper(countryregioncode)) AS country_region_code,
        trim(name) AS state_province_name
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND stateprovinceid IS NOT NULL
        AND trim(name) IS NOT NULL
        AND trim(name) != ''
)

SELECT * FROM renamed
