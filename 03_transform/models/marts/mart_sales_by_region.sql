-- ============================================================================
-- mart_sales_by_region.sql — Mart de ventas por region
-- ============================================================================
-- KPIs por estado brasileno para el dashboard ejecutivo.
-- Este mart responde: "Como estan las ventas por region?"
--
-- Se materializa como TABLE (no VIEW) para que Power BI lo lea rapido.
-- ============================================================================

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_items_joined') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    c.customer_state,
    c.customer_city,

    -- Metricas de volumen
    COUNT(DISTINCT o.order_id)  AS total_orders,
    COUNT(*)                    AS total_items,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers,

    -- Metricas financieras
    ROUND(SUM(o.price), 2)                          AS total_revenue,
    ROUND(SUM(o.freight_value), 2)                   AS total_freight,
    ROUND(SUM(o.price) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value,
    ROUND(AVG(o.price), 2)                           AS avg_item_price,

    -- Metricas de satisfaccion
    ROUND(AVG(o.review_score), 2)   AS avg_review_score,

    -- Metricas de entrega
    ROUND(AVG(o.days_to_deliver), 1)       AS avg_days_to_deliver,
    ROUND(AVG(o.delivery_delay_days), 1)   AS avg_delivery_delay,
    SUM(CASE WHEN o.delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND(100.0 * SUM(CASE WHEN o.delivery_delay_days > 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(o.days_to_deliver), 0), 1) AS pct_late

FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state, c.customer_city
