-- ============================================================================
-- fact_orders.sql — Poblar la tabla de hechos
-- ============================================================================
-- Esta es la query mas compleja del proyecto. Une 6 tablas staging para
-- construir la fact table con granularidad de ORDER ITEM.
--
-- JOINs realizados:
--   1. stg_order_items (base, define la granularidad)
--   2. stg_orders (estado, fechas de la orden)
--   3. stg_customers -> dim_customers (obtener surrogate key)
--   4. dim_products (obtener surrogate key)
--   5. dim_sellers (obtener surrogate key)
--   6. stg_order_payments (pago total por orden, agregado)
--   7. stg_order_reviews (score por orden)
-- ============================================================================

INSERT INTO warehouse.fact_orders
WITH order_payments AS (
    -- Agregar el pago total por orden (un pedido puede tener multiples pagos:
    -- ej. parte con tarjeta, parte con boleto)
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM staging.stg_order_payments
    GROUP BY order_id
),
order_reviews AS (
    -- Tomar el score de review por orden (si hay multiples, promediamos)
    SELECT
        order_id,
        ROUND(AVG(review_score)) AS review_score
    FROM staging.stg_order_reviews
    GROUP BY order_id
)
SELECT
    -- Claves
    oi.order_id,
    oi.order_item_id,

    -- Surrogate keys (lookup a dimensiones)
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    CAST(STRFTIME(CAST(o.order_purchase_timestamp AS DATE), '%Y%m%d') AS INTEGER) AS order_date_key,

    -- Estado
    o.order_status,

    -- Metricas financieras
    oi.price,
    oi.freight_value,
    pay.total_payment   AS payment_value,

    -- Satisfaccion
    rev.review_score,

    -- Fechas para metricas de entrega
    CAST(o.order_purchase_timestamp AS DATE)      AS order_purchase_date,
    CAST(o.order_delivered_customer_date AS DATE)  AS order_delivered_date,
    CAST(o.order_estimated_delivery_date AS DATE)  AS order_estimated_date,

    -- Metricas de entrega calculadas (en dias)
    -- DATE_DIFF calcula la diferencia entre dos fechas
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN DATE_DIFF('day',
                CAST(o.order_purchase_timestamp AS DATE),
                CAST(o.order_delivered_customer_date AS DATE))
        ELSE NULL
    END AS days_to_deliver,

    CASE
        WHEN o.order_estimated_delivery_date IS NOT NULL
        THEN DATE_DIFF('day',
                CAST(o.order_purchase_timestamp AS DATE),
                CAST(o.order_estimated_delivery_date AS DATE))
        ELSE NULL
    END AS days_estimated,

    -- Delay: positivo = llego tarde, negativo = llego temprano
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
        THEN DATE_DIFF('day',
                CAST(o.order_estimated_delivery_date AS DATE),
                CAST(o.order_delivered_customer_date AS DATE))
        ELSE NULL
    END AS delivery_delay_days

FROM staging.stg_order_items oi

-- JOIN con ordenes (fechas, estado, customer_id)
INNER JOIN staging.stg_orders o
    ON oi.order_id = o.order_id

-- JOIN con customers staging para obtener customer_unique_id,
-- luego lookup a dim_customers para obtener surrogate key
INNER JOIN staging.stg_customers c
    ON o.customer_id = c.customer_id
INNER JOIN warehouse.dim_customers dc
    ON c.customer_unique_id = dc.customer_unique_id

-- Lookup a dim_products para surrogate key
INNER JOIN warehouse.dim_products dp
    ON oi.product_id = dp.product_id

-- Lookup a dim_sellers para surrogate key
INNER JOIN warehouse.dim_sellers ds
    ON oi.seller_id = ds.seller_id

-- Pagos y reviews (LEFT JOIN porque no todos los pedidos tienen ambos)
LEFT JOIN order_payments pay
    ON oi.order_id = pay.order_id
LEFT JOIN order_reviews rev
    ON oi.order_id = rev.order_id;
