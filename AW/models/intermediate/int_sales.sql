-- int_fact_sales: Fato de vendas
-- Foco: Integração de detalhes e cabeçalhos com dimensões relacionadas
-- Materialização: incremental para performance

WITH stg_order_details AS (
    SELECT * FROM {{ ref('stg_databricks__salesorderdetail_raw') }}
),

stg_order_headers AS (
    SELECT * FROM {{ ref('stg_databricks__salesorderheader_raw') }}
),

stg_reasons_mapping AS (
    SELECT * FROM {{ ref('stg_databricks__sales_salesorderheadersalesreason_raw') }}
),

-- Primeira razão por pedido para evitar duplicação
primary_reason AS (
    SELECT
        sales_order_id,
        sales_reason_id,
        row_number() OVER (PARTITION BY sales_order_id ORDER BY sales_reason_id) AS rn
    FROM stg_reasons_mapping
),

first_reason_only AS (
    SELECT
        sales_order_id,
        sales_reason_id
    FROM primary_reason
    WHERE rn = 1
),

sales_fact_integrated AS (
    SELECT
        d.sales_order_id,
        d.sales_order_detail_id,
        d.product_id,
        h.customer_id,
        -- Tratar NULLs em sales_person_id (mapear para 0 = Unknown)
        h.territory_id,
        cast(date_format(h.order_date, 'yyyyMMdd') AS bigint) AS order_date_id,
        -- Converter data para surrogate key formato YYYYMMDD
        d.special_offer_id,
        h.total_due,
        -- Traduzir valores nulos para razão padrão (ID 1)
        d.line_total,
        d.order_quantity,
        d.unit_price,
        coalesce(h.sales_person_id, 0) AS sales_person_id,
        coalesce(fr.sales_reason_id, 1) AS sales_reason_id
    FROM stg_order_details AS d
    INNER JOIN stg_order_headers AS h ON d.sales_order_id = h.sales_order_id
    LEFT JOIN first_reason_only AS fr ON h.sales_order_id = fr.sales_order_id
)

SELECT * FROM sales_fact_integrated
