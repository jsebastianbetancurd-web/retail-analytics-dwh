-- ============================================================================
-- mart_product_performance.sql — Mart de performance por producto/categoria
-- ============================================================================
-- KPIs por categoria de producto para el dashboard de merchandising.
-- Responde: "Que categorias venden mas? Cuales tienen mejor satisfaccion?"
--
-- Usa el modelo intermedio de productos enriquecidos para obtener
-- la categoria en ingles y la banda de precio.
-- ============================================================================

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_items_joined') }}
),

products AS (
    SELECT * FROM {{ ref('int_products_enriched') }}
)

SELECT
    p.category_en,
    p.price_band,

    -- Metricas de volumen
    COUNT(DISTINCT o.order_id)      AS total_orders,
    SUM(1)                          AS total_items_sold,
    COUNT(DISTINCT p.product_id)    AS unique_products,

    -- Metricas financieras
    ROUND(SUM(o.price), 2)      AS total_revenue,
    ROUND(AVG(o.price), 2)      AS avg_item_price,
    ROUND(SUM(o.freight_value), 2) AS total_freight,

    -- Satisfaccion
    ROUND(AVG(o.review_score), 2) AS avg_review_score,

    -- Peso promedio (relevante para logistica)
    ROUND(AVG(p.product_weight_g), 0) AS avg_weight_g

FROM orders o
INNER JOIN products p ON o.product_id = p.product_id
GROUP BY p.category_en, p.price_band
