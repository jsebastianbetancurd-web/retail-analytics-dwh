-- ============================================================================
-- int_orders_items_joined.sql — Ordenes enriquecidas con items y pagos
-- ============================================================================
-- Este es el JOIN mas comun en retail analytics: unir ordenes con sus items
-- y pagos. El resultado tiene una fila por ORDER ITEM con toda la informacion
-- necesaria para calcular revenue, ticket promedio, etc.
--
-- Nota: usamos ref() en vez de nombres de tabla hardcodeados.
-- dbt resuelve las dependencias automaticamente.
-- ============================================================================

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

-- Agregamos los pagos a nivel de orden (suma total por orden)
payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value,
        -- Metodo de pago mas usado en la orden
        MODE(payment_type)  AS main_payment_type
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

-- Score de review por orden
reviews_agg AS (
    SELECT
        order_id,
        ROUND(AVG(review_score)) AS review_score
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
)

SELECT
    -- Identificadores
    o.order_id,
    i.order_item_id,
    o.customer_id,
    i.product_id,
    i.seller_id,

    -- Estado y fechas
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Metricas financieras (nivel item)
    i.price,
    i.freight_value,
    i.price + i.freight_value AS total_item_value,

    -- Metricas de pago (nivel orden, denormalizado)
    p.total_payment_value,
    p.main_payment_type,

    -- Satisfaccion (nivel orden, denormalizado)
    r.review_score,

    -- Metricas de entrega calculadas
    CAST(o.order_purchase_timestamp AS DATE) AS purchase_date,
    CAST(o.order_delivered_customer_date AS DATE) AS delivered_date,
    CAST(o.order_estimated_delivery_date AS DATE) AS estimated_date,

    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN DATE_DIFF('day',
                CAST(o.order_purchase_timestamp AS DATE),
                CAST(o.order_delivered_customer_date AS DATE))
    END AS days_to_deliver,

    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
        THEN DATE_DIFF('day',
                CAST(o.order_estimated_delivery_date AS DATE),
                CAST(o.order_delivered_customer_date AS DATE))
    END AS delivery_delay_days

FROM items i
INNER JOIN orders o ON i.order_id = o.order_id
LEFT JOIN payments_agg p ON o.order_id = p.order_id
LEFT JOIN reviews_agg r ON o.order_id = r.order_id
