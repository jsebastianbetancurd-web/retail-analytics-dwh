-- ============================================================================
-- dim_customers.sql — Poblar la dimension de clientes
-- ============================================================================
-- Usamos customer_unique_id como clave natural (el ID real del cliente).
-- Nota: customer_id es un ID por transaccion en Olist, no por cliente.
-- Un mismo customer_unique_id puede tener multiples customer_id.
--
-- Tomamos la ciudad/estado de la transaccion mas reciente del cliente.
-- ============================================================================

INSERT INTO warehouse.dim_customers
WITH ranked_customers AS (
    -- Para cada customer_unique_id, tomamos los datos mas recientes
    -- usando la fecha de compra como criterio de ordenacion
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp DESC
        ) AS rn
    FROM staging.stg_customers c
    INNER JOIN staging.stg_orders o ON c.customer_id = o.customer_id
)
SELECT
    -- Surrogate key: entero autoincrementable usando ROW_NUMBER
    ROW_NUMBER() OVER (ORDER BY customer_unique_id) AS customer_key,
    customer_unique_id,
    customer_city,
    customer_state
FROM ranked_customers
WHERE rn = 1;  -- Solo la version mas reciente de cada cliente
