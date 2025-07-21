-- fct_purchases: Tabela fato de compras para BI
-- Métricas de negócio para análise de custos e supply chain

SELECT
    purchase_order_id,
    purchase_order_detail_id,
    product_id,
    purchase_date_id,
    line_total,
    unit_price,
    order_quantity
FROM {{ ref('int_purchases') }}
