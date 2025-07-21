-- dim_date: Dimensão de datas para BI
-- Chaves de relacionamentos (OrderDateID) para star schema

SELECT
    order_date_id,
    date,
    year,
    quarter,
    month,
    day
FROM {{ ref('int_date') }}
