-- int_special_offers: Dimensão de ofertas especiais
-- Foco: Padronização de ofertas e descontos
-- Materialização: table

WITH stg_special_offers AS (
    SELECT * FROM {{ ref('stg_databricks__sales_specialoffer_raw') }}
)

SELECT * FROM stg_special_offers
