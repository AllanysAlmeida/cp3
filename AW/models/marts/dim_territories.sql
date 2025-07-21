-- dim_territories: Dimensão de territórios para BI

SELECT
    territory_id,
    territory_name,
    country_region_code,
    country_region_name
FROM {{ ref('int_territories') }}
