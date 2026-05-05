-- ============================================================================
-- dim_products.sql — Poblar la dimension de productos
-- ============================================================================
-- Enriquece los datos de productos con:
--   1. Traduccion de la categoria al ingles (JOIN con stg_category_translation)
--   2. Precio promedio del producto (calculado desde stg_order_items)
--   3. Banda de precio (segmentacion para dashboards)
--
-- Las bandas de precio facilitan el analisis en Power BI sin que el usuario
-- tenga que escribir formulas DAX para segmentar.
-- ============================================================================

INSERT INTO warehouse.dim_products
WITH product_prices AS (
    -- Calcular el precio promedio de cada producto basado en sus ventas.
    -- Usamos AVG porque un mismo producto puede venderse a distintos precios
    -- (ej. descuentos, variaciones regionales)
    SELECT
        product_id,
        AVG(price) AS avg_price
    FROM staging.stg_order_items
    GROUP BY product_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_key,
    p.product_id,
    p.product_category_name                  AS product_category_pt,
    t.product_category_name_english          AS product_category_en,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    pp.avg_price,
    -- Banda de precio calculada con CASE
    -- Estos umbrales son basados en el analisis del dataset Olist
    CASE
        WHEN pp.avg_price IS NULL        THEN 'sin_ventas'
        WHEN pp.avg_price < 50           THEN 'budget'       -- < 50 BRL
        WHEN pp.avg_price < 150          THEN 'mid_range'    -- 50-149 BRL
        WHEN pp.avg_price < 500          THEN 'premium'      -- 150-499 BRL
        ELSE                                  'luxury'       -- 500+ BRL
    END AS price_band
FROM staging.stg_products p
LEFT JOIN staging.stg_category_translation t
    ON p.product_category_name = t.product_category_name
LEFT JOIN product_prices pp
    ON p.product_id = pp.product_id;
