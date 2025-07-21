-- int_date: Dimensão de datas
-- Foco: Calendário completo para análise temporal
-- Materialização: table

WITH seq AS (
    SELECT explode(sequence(0, 7304)) AS n  -- 20 anos * 365.25 dias aproximadamente
),

date_range AS (
    -- Gerar spine de datas usando explode e sequence
    SELECT date_add('2011-01-01', seq.n) AS date_day
    FROM seq
    WHERE date_add('2011-01-01', seq.n) <= date('2030-12-31')
),

date_dimension AS (
    SELECT
        cast(date_format(date_day, 'yyyyMMdd') AS bigint) AS order_date_id,
        date_day AS date,
        year(date_day) AS year,
        quarter(date_day) AS quarter,
        month(date_day) AS month,
        day(date_day) AS day
    FROM date_range
)

SELECT * FROM date_dimension
