-- int_sales_reasons: Dimensão de motivos de venda
-- Foco: Classificação e padronização dos motivos
-- Materialização: table

WITH stg_sales_reasons AS (
    SELECT * FROM {{ ref('stg_databricks__sales_salesreason_raw') }}
)

SELECT * FROM stg_sales_reasons
