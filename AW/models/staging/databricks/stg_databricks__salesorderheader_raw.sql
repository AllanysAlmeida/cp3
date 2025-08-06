WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'salesorderheader') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number()
            OVER (PARTITION BY salesorderid ORDER BY modifieddate DESC)
            AS row_num
    FROM source
    WHERE salesorderid IS NOT null
),

renamed AS (
    SELECT
        cast(salesorderid AS integer) AS sales_order_id,
        cast(orderdate AS date) AS order_date,
        cast(totaldue AS numeric(19, 4)) AS total_due,
        cast(customerid AS integer) AS customer_id,
        cast(territoryid AS integer) AS territory_id,
        cast(creditcardid AS integer) AS credit_card_id,
        DATEPART(QUARTER, orderdate) AS order_quarter,
        DATEPART(YEAR, orderdate) AS order_year,
        CASE WHEN onlineorderflag = 1 THEN TRUE ELSE FALSE END AS online_order_flag
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND salesorderid IS NOT NULL
        AND customerid IS NOT NULL
        AND orderdate >= date('2005-01-01')
        AND orderdate <= current_date()
        AND totaldue >= 0
)

SELECT * FROM renamed
