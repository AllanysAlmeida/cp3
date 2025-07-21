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
        coalesce(cast(salesorderid AS integer), 0) AS sales_order_id,
        coalesce(cast(customerid AS integer), 0) AS customer_id,
        coalesce(cast(salespersonid AS integer), 0) AS sales_person_id,
        coalesce(cast(territoryid AS integer), 0) AS territory_id,
        coalesce(cast(orderdate AS date), date('1900-01-01')) AS order_date,
        coalesce(cast(totaldue AS numeric(19, 4)), 0) AS total_due
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(salesorderid, 0) > 0
        AND coalesce(customerid, 0) > 0
        AND coalesce(orderdate, date('1900-01-01')) >= date('2005-01-01')
        AND coalesce(orderdate, date('1900-01-01')) <= current_date()
        AND coalesce(totaldue, 0) >= 0
)

SELECT * FROM renamed
