WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'person_address') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY addressid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE addressid IS NOT null
),

renamed AS (
    SELECT
        cast(addressid AS integer) AS address_id,
        cast(stateprovinceid AS integer) AS state_province_id,
        initcap(trim(city)) AS city
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND addressid IS NOT NULL
        AND trim(city) IS NOT NULL
        AND trim(city) != ''
)

SELECT * FROM renamed
