WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'person_person') }}
),

filtered AS (
    SELECT coalesce(cast(businessentityid AS integer), 0) AS business_entity_id
    FROM source
    WHERE true
),

deduped AS (
    SELECT
        business_entity_id,
        row_number() OVER (PARTITION BY business_entity_id ORDER BY business_entity_id) AS rn
    FROM filtered
),

final AS (
    SELECT business_entity_id
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM final
