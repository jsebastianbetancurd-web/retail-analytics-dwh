-- ============================================================================
-- mart_delivery_kpis.sql — Mart de KPIs de logistica/entrega
-- ============================================================================
-- KPIs de delivery por estado y mes para el dashboard de operaciones.
-- Responde: "Donde y cuando tenemos problemas de entrega?"
--
-- Incluye metricas de on-time delivery rate, tiempo promedio, y retrasos.
-- ============================================================================

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_items_joined') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    c.customer_state,

    -- Periodo temporal
    YEAR(CAST(o.order_purchase_timestamp AS DATE))  AS order_year,
    MONTH(CAST(o.order_purchase_timestamp AS DATE)) AS order_month,

    -- Volumen
    COUNT(DISTINCT o.order_id) AS total_orders,

    -- Metricas de entrega (solo ordenes entregadas)
    COUNT(CASE WHEN o.days_to_deliver IS NOT NULL THEN 1 END) AS delivered_orders,
    ROUND(AVG(o.days_to_deliver), 1) AS avg_days_to_deliver,
    MIN(o.days_to_deliver) AS min_days_to_deliver,
    MAX(o.days_to_deliver) AS max_days_to_deliver,

    -- On-time delivery
    SUM(CASE WHEN o.delivery_delay_days <= 0 THEN 1 ELSE 0 END) AS on_time_deliveries,
    SUM(CASE WHEN o.delivery_delay_days > 0 THEN 1 ELSE 0 END)  AS late_deliveries,
    ROUND(100.0 * SUM(CASE WHEN o.delivery_delay_days <= 0 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(o.days_to_deliver), 0), 1) AS on_time_rate_pct,

    -- Delay promedio (solo retrasos)
    ROUND(AVG(CASE WHEN o.delivery_delay_days > 0
                   THEN o.delivery_delay_days END), 1) AS avg_delay_when_late,

    -- Satisfaccion vs entrega
    ROUND(AVG(o.review_score), 2) AS avg_review_score

FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state, order_year, order_month
