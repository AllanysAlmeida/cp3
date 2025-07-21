-- int_territories: Dimensão de territórios de vendas
-- Foco: Integrar territórios com informações geográficas
-- Materialização: table

WITH stg_territories AS (
    SELECT * FROM {{ ref('stg_databricks__sales_salesterritory_raw') }}
),

stg_countries AS (
    SELECT * FROM {{ ref('stg_databricks__person_countryregion_raw') }}
)

SELECT
    t.territory_id,
    t.territory_name,
    t.country_region_code,
    c.country_region_name
FROM stg_territories AS t
LEFT JOIN stg_countries AS c ON t.country_region_code = c.country_region_code
