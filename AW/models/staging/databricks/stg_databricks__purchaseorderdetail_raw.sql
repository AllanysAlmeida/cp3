WITH source AS (
    SELECT * FROM {{ source('databricks_source', 'purchaseorderdetail') }}
),

deduplicated_source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY purchaseorderdetailid ORDER BY modifieddate DESC) AS row_num
    FROM source
    WHERE purchaseorderdetailid IS NOT null
),

renamed AS (
    SELECT
        coalesce(cast(purchaseorderdetailid AS integer), 0) AS purchase_order_detail_id,
        coalesce(cast(purchaseorderid AS integer), 0) AS purchase_order_id,
        coalesce(cast(productid AS integer), 0) AS product_id,
        coalesce(cast(orderqty AS integer), 0) AS order_quantity,
        coalesce(cast(unitprice AS numeric(19, 4)), 0) AS unit_price,
        coalesce(cast(linetotal AS numeric(19, 4)), 0) AS line_total
    FROM deduplicated_source
    WHERE
        row_num = 1
        AND coalesce(purchaseorderdetailid, 0) > 0
        AND coalesce(purchaseorderid, 0) > 0
        AND coalesce(productid, 0) > 0
        AND coalesce(orderqty, 0) > 0
        AND coalesce(unitprice, 0) >= 0
        AND coalesce(linetotal, 0) >= 0
)

SELECT * FROM renamed
