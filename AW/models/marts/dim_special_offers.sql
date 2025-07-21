-- dim_special_offers: Dimensão de ofertas especiais para BI

SELECT
    special_offer_id,
    discount_percentage,
    offer_type
FROM {{ ref('int_special_offers') }}
