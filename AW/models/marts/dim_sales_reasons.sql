-- dim_sales_reasons: Dimensão de motivos de venda para BI

SELECT
    sales_reason_id,
    sales_reason_name,
    reason_type
FROM {{ ref('int_sales_reasons') }}
