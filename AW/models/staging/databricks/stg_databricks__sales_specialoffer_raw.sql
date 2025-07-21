WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_specialoffer') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY specialofferid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE specialofferid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(specialofferid AS integer), 0) AS special_offer_id,
        coalesce(cast(discountpct AS numeric(19, 4)), 0) AS discount_percentage,
        coalesce(trim(upper(type)), 'Unknown') AS offer_type
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(specialofferid, 0) > 0
        AND coalesce(discountpct, 0) >= 0
        AND coalesce(discountpct, 0) <= 1
)

SELECT * FROM renamed
