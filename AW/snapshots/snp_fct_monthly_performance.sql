{% snapshot snp_fct_monthly_performance %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='performance_unique_key',
      check_cols=[
          'total_revenue',
          'total_cost_of_goods_sold',
          'total_gross_profit',
          'total_orders',
          'total_products_sold',
          'total_products_purchased',
          'total_purchase_cost',
          'sales_quota_target'
      ]
    )
}}

WITH sales_monthly AS (
    SELECT
        CAST(date_format(d.date, 'yyyyMM') AS INT) AS month_year,
        COALESCE(s.territory_id, 0) AS territory_id,
        COALESCE(p.product_category_id, 0) AS product_category_id,
        COALESCE(s.sales_person_id, 0) AS business_entity_id,
        SUM(s.line_total) AS total_revenue,
        SUM(s.order_quantity * p.standard_cost) AS total_cost_of_goods_sold,
        COUNT(DISTINCT s.sales_order_id) AS total_orders,
        SUM(s.order_quantity) AS total_products_sold
    FROM {{ ref('int_sales') }} s
    INNER JOIN {{ ref('int_date') }} d ON s.order_date_id = d.order_date_id
    INNER JOIN {{ ref('int_products') }} p ON s.product_id = p.product_id
    GROUP BY 1, 2, 3, 4
),

purchases_monthly AS (
    SELECT
        CAST(date_format(d.date, 'yyyyMM') AS INT) AS month_year,
        COALESCE(p.product_category_id, 0) AS product_category_id,
        SUM(pu.order_quantity) AS total_products_purchased,
        SUM(pu.line_total) AS total_purchase_cost
    FROM {{ ref('int_purchases') }} pu
    INNER JOIN {{ ref('int_date') }} d ON pu.purchase_date_id = d.order_date_id
    INNER JOIN {{ ref('int_products') }} p ON pu.product_id = p.product_id
    GROUP BY 1, 2
),

sales_quota_monthly AS (
    SELECT
        CAST(date_format(d.date, 'yyyyMM') AS INT) AS month_year,
        sp.business_entity_id,
        AVG(sp.sales_quota) AS sales_quota_target
    FROM {{ ref('int_sales_persons') }} sp
    CROSS JOIN {{ ref('int_date') }} d
    GROUP BY 1, 2
)

SELECT
    CONCAT(
        s.month_year, '-',
        s.territory_id, '-',
        s.product_category_id, '-',
        s.business_entity_id
    ) AS performance_unique_key,
    
    s.month_year,
    s.territory_id,
    s.product_category_id,
    s.business_entity_id,
    
    s.total_revenue,
    s.total_cost_of_goods_sold,
    (s.total_revenue - s.total_cost_of_goods_sold) AS total_gross_profit,
    s.total_orders,
    s.total_products_sold,
    
    COALESCE(p.total_products_purchased, 0) AS total_products_purchased,
    COALESCE(p.total_purchase_cost, 0) AS total_purchase_cost,
    
    COALESCE(sq.sales_quota_target, 0) AS sales_quota_target

FROM sales_monthly s
LEFT JOIN purchases_monthly p ON s.month_year = p.month_year AND s.product_category_id = p.product_category_id
LEFT JOIN sales_quota_monthly sq ON s.month_year = sq.month_year AND s.business_entity_id = sq.business_entity_id

{% endsnapshot %}