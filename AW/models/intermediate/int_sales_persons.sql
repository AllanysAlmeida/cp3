-- int_sales_persons: Dimensão de vendedores
-- Foco: Dados limpos dos vendedores com métricas de performance
-- Materialização: table

WITH stg_sales_persons AS (
    SELECT * FROM {{ ref('stg_databricks__sales_salesperson_raw') }}
),

stg_persons AS (
    SELECT * FROM {{ ref('stg_databricks__person_person_raw') }}
)

SELECT
    s.business_entity_id,
    s.commission_pct,
    s.sales_quota
FROM stg_sales_persons AS s
LEFT JOIN stg_persons AS p ON s.business_entity_id = p.business_entity_id
