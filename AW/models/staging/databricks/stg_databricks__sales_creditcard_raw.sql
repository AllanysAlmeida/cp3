WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'sales_creditcard') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY creditcardid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE creditcardid IS NOT null
),

renamed AS (
    SELECT
        cast(creditcardid AS integer) AS credit_card_id,
        trim(cardtype) AS card_type
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND creditcardid IS NOT NULL
        AND trim(cardtype) IS NOT NULL
        AND trim(cardtype) != ''
)

SELECT * FROM renamed
