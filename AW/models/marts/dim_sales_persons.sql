-- dim_sales_persons: Dimensão de vendedores para BI
-- Garantir testes rigorosos (chaves primárias, consistência)

WITH sales_persons_base AS (
    SELECT
        business_entity_id,
        commission_pct,
        sales_quota
    FROM {{ ref('int_sales_persons') }}
),

unknown_sales_person AS (
    SELECT
        0 AS business_entity_id,
        0.0 AS commission_pct,
        0.0 AS sales_quota
)

SELECT * FROM sales_persons_base
UNION ALL
SELECT * FROM unknown_sales_person
