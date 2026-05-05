-- ============================================================================
-- int_products_enriched.sql — Productos enriquecidos con categoria y precio
-- ============================================================================
-- Enriquece el catalogo de productos con:
--   1. Traduccion de categoria al ingles
--   2. Precio promedio historico (calculado de las ventas)
--   3. Banda de precio para segmentacion en dashboards

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

translations AS (
    SELECT * FROM {{ ref('stg_category_translation') }}
),

-- Precio promedio por producto (basado en ventas reales)
product_prices AS (
    SELECT
        product_id,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(*) AS total_items_sold
    FROM {{ ref('stg_order_items') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_category_name                             AS category_pt,
    COALESCE(t.product_category_name_english, 'other')  AS category_en,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    pp.avg_price,
    COALESCE(pp.total_items_sold, 0) AS total_items_sold,

    -- Banda de precio para segmentacion
    CASE
        WHEN pp.avg_price IS NULL   THEN 'no_sales'
        WHEN pp.avg_price < 50      THEN 'budget'
        WHEN pp.avg_price < 150     THEN 'mid_range'
        WHEN pp.avg_price < 500     THEN 'premium'
        ELSE                             'luxury'
    END AS price_band

FROM products p
LEFT JOIN translations t ON p.product_category_name = t.product_category_name
LEFT JOIN product_prices pp ON p.product_id = pp.product_id
