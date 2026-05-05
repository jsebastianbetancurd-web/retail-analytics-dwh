-- ============================================================================
-- dim_date.sql — Poblar la dimension de calendario
-- ============================================================================
-- Genera todas las fechas entre la primera y ultima orden del dataset.
-- DuckDB tiene la funcion generate_series() que crea secuencias de fechas.
-- ============================================================================

INSERT INTO warehouse.dim_date
WITH date_range AS (
    -- Obtener rango de fechas del dataset (primera y ultima compra)
    SELECT
        MIN(CAST(order_purchase_timestamp AS DATE)) AS min_date,
        MAX(CAST(order_purchase_timestamp AS DATE)) AS max_date
    FROM staging.stg_orders
),
all_dates AS (
    -- Generar todas las fechas del rango usando generate_series
    SELECT UNNEST(generate_series(
        (SELECT min_date FROM date_range),
        (SELECT max_date FROM date_range),
        INTERVAL 1 DAY
    ))::DATE AS full_date
)
SELECT
    -- date_key en formato YYYYMMDD (entero)
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    YEAR(full_date)                                 AS year,
    QUARTER(full_date)                              AS quarter,
    MONTH(full_date)                                AS month,
    MONTHNAME(full_date)                            AS month_name,
    WEEKOFYEAR(full_date)                           AS week_of_year,
    DAY(full_date)                                  AS day_of_month,
    DAYOFWEEK(full_date)                            AS day_of_week,    -- 0=Lunes
    DAYNAME(full_date)                              AS day_name,
    DAYOFWEEK(full_date) >= 5                       AS is_weekend      -- 5=Sab, 6=Dom
FROM all_dates
ORDER BY full_date;
