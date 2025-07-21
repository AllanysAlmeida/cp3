SELECT
    sales_order_id,
    sales_order_detail_id,
    product_id,
    customer_id,
    territory_id,
    order_date_id,
    special_offer_id,
    sales_reason_id,
    total_due,
    line_total,
    order_quantity,
    unit_price,
    coalesce(sales_person_id, 0) AS business_entity_id
FROM {{ ref('int_sales') }}
