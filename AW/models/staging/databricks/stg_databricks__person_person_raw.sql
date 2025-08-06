WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'person_person') }}
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
        cast(businessentityid AS integer) AS business_entity_id,
        trim(firstname) AS first_name,
        trim(lastname) AS last_name,
        trim(persontype) AS person_type,
        CONCAT(trim(firstname), ' ', trim(lastname)) AS full_name
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND businessentityid IS NOT NULL
)

SELECT * FROM renamed
