-- int_fact_purchases: Fato de compras
-- Foco: Integração de detalhes e cabeçalhos de ordens de compra
-- Materialização: incremental

WITH stg_purchase_details AS (
    SELECT * FROM {{ ref('stg_databricks__purchaseorderdetail_raw') }}
),

stg_purchase_headers AS (
    SELECT * FROM {{ ref('stg_databricks__purchaseorderheader_raw') }}
),

purchases_fact_integrated AS (
    SELECT
        pd.purchase_order_id,
        pd.purchase_order_detail_id,
        pd.product_id,
        -- Converter data para surrogate key formato YYYYMMDD
        cast(date_format(ph.order_date, 'yyyyMMdd') AS bigint) AS purchase_date_id,
        pd.line_total,
        pd.unit_price,
        pd.order_quantity
    FROM stg_purchase_details AS pd
    INNER JOIN stg_purchase_headers AS ph ON pd.purchase_order_id = ph.purchase_order_id
)

SELECT * FROM purchases_fact_integrated
