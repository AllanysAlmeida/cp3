WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'salesorderdetail') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY salesorderdetailid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE salesorderdetailid IS NOT null
),

renamed AS (
    SELECT
        cast(salesorderdetailid AS integer) AS sales_order_detail_id,
        cast(salesorderid AS integer) AS sales_order_id,
        cast(productid AS integer) AS product_id,
        cast(orderqty AS integer) AS order_qty,
        cast(linetotal AS numeric(19, 4)) AS line_total,
        cast(unitprice AS numeric(19, 4)) AS unit_price
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND salesorderdetailid IS NOT NULL
        AND salesorderid IS NOT NULL
        AND productid IS NOT NULL
        AND orderqty > 0
        AND linetotal >= 0
)

SELECT * FROM renamed
