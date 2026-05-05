-- ============================================================================
-- create_schema.sql — DDL del Star Schema para el Retail Analytics DWH
-- ============================================================================
-- Este archivo define la estructura del modelo dimensional (star schema).
-- Granularidad de fact_orders: UNA FILA = UN ITEM DE UNA ORDEN.
--
-- Decisiones de diseno documentadas:
--   1. Star schema (no snowflake): prioriza velocidad de lectura y simplicidad
--   2. Surrogate keys (INTEGER): mas rapidos que JOINs con VARCHAR(32)
--   3. dim_date como tabla calendario: permite analisis temporal sin funciones
--   4. Metricas de entrega denormalizadas en fact_orders: evita JOINs extra
--
-- Ejecucion: Este DDL es ejecutado por build_warehouse.py
-- ============================================================================

-- Crear schema para el modelo dimensional
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================================================
-- DIMENSIONES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- dim_date: Dimension de calendario
-- ---------------------------------------------------------------------------
-- Tabla calendario pre-generada con todos los dias del rango del dataset.
-- Permite filtrar por anio, mes, dia de la semana, fin de semana, etc.
-- sin necesidad de funciones DATE_PART() en cada query.
--
-- La clave es un entero YYYYMMDD (ej: 20170101) — comun en DWH porque
-- es legible por humanos y eficiente para particionamiento.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE warehouse.dim_date (
    date_key        INTEGER PRIMARY KEY,    -- YYYYMMDD como entero (ej: 20170615)
    full_date       DATE NOT NULL,          -- Fecha completa
    year            INTEGER NOT NULL,       -- 2016, 2017, 2018
    quarter         INTEGER NOT NULL,       -- 1, 2, 3, 4
    month           INTEGER NOT NULL,       -- 1..12
    month_name      VARCHAR NOT NULL,       -- 'January', 'February', ...
    week_of_year    INTEGER NOT NULL,       -- 1..53
    day_of_month    INTEGER NOT NULL,       -- 1..31
    day_of_week     INTEGER NOT NULL,       -- 0=Lunes .. 6=Domingo
    day_name        VARCHAR NOT NULL,       -- 'Monday', 'Tuesday', ...
    is_weekend      BOOLEAN NOT NULL        -- true si sabado o domingo
);

-- ---------------------------------------------------------------------------
-- dim_customers: Dimension de clientes
-- ---------------------------------------------------------------------------
-- Basada en customer_unique_id (el ID real del cliente), no customer_id
-- (que es un ID por transaccion). Multiples customer_id pueden mapear
-- a un solo customer_unique_id.
--
-- Tomamos la ciudad/estado de la transaccion mas reciente del cliente.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE warehouse.dim_customers (
    customer_key        INTEGER PRIMARY KEY,    -- Surrogate key
    customer_unique_id  VARCHAR NOT NULL,       -- Clave natural del sistema fuente
    customer_city       VARCHAR NOT NULL,       -- Ciudad (normalizada a minusculas)
    customer_state      VARCHAR(2) NOT NULL     -- Estado brasileno (UF, 2 letras)
);

-- ---------------------------------------------------------------------------
-- dim_products: Dimension de productos
-- ---------------------------------------------------------------------------
-- Incluye la categoria traducida al ingles (JOIN con tabla de traduccion)
-- y una banda de precio calculada para facilitar segmentacion en dashboards.
--
-- Bandas de precio:
--   'budget'    = precio promedio < 50 BRL
--   'mid_range' = precio promedio 50-150 BRL
--   'premium'   = precio promedio 150-500 BRL
--   'luxury'    = precio promedio > 500 BRL
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE warehouse.dim_products (
    product_key                 INTEGER PRIMARY KEY,    -- Surrogate key
    product_id                  VARCHAR NOT NULL,       -- Clave natural
    product_category_pt         VARCHAR NOT NULL,       -- Categoria en portugues
    product_category_en         VARCHAR,                -- Categoria en ingles
    product_weight_g            DOUBLE,                 -- Peso en gramos
    product_length_cm           DOUBLE,                 -- Longitud
    product_height_cm           DOUBLE,                 -- Altura
    product_width_cm            DOUBLE,                 -- Ancho
    avg_price                   DOUBLE,                 -- Precio promedio del producto
    price_band                  VARCHAR                 -- 'budget', 'mid_range', 'premium', 'luxury'
);

-- ---------------------------------------------------------------------------
-- dim_sellers: Dimension de vendedores
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE warehouse.dim_sellers (
    seller_key          INTEGER PRIMARY KEY,    -- Surrogate key
    seller_id           VARCHAR NOT NULL,       -- Clave natural
    seller_city         VARCHAR NOT NULL,       -- Ciudad (normalizada)
    seller_state        VARCHAR(2) NOT NULL     -- Estado brasileno (UF)
);

-- ============================================================================
-- TABLA DE HECHOS
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fact_orders: Tabla de hechos de ordenes (granularidad: ORDER ITEM)
-- ---------------------------------------------------------------------------
-- Cada fila representa UN ITEM dentro de UNA ORDEN.
-- Una orden con 3 productos genera 3 filas en esta tabla.
--
-- Metricas:
--   - price: valor del item (revenue)
--   - freight_value: costo de envio del item
--   - payment_value: pago total de la orden (denormalizado al nivel de item)
--   - review_score: calificacion del cliente (1-5, denormalizado)
--
-- Metricas calculadas de entrega:
--   - days_to_deliver: dias reales entre compra y entrega
--   - days_estimated: dias estimados entre compra y entrega estimada
--   - delivery_delay_days: diferencia (positivo = retraso, negativo = temprano)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE warehouse.fact_orders (
    -- Claves de la fact table
    order_id                VARCHAR NOT NULL,       -- ID de la orden
    order_item_id           INTEGER NOT NULL,       -- Secuencial del item dentro de la orden

    -- Foreign keys a dimensiones (surrogate keys)
    customer_key            INTEGER NOT NULL,       -- FK -> dim_customers
    product_key             INTEGER NOT NULL,       -- FK -> dim_products
    seller_key              INTEGER NOT NULL,       -- FK -> dim_sellers
    order_date_key          INTEGER NOT NULL,       -- FK -> dim_date (fecha de compra)

    -- Estado de la orden
    order_status            VARCHAR NOT NULL,       -- delivered, shipped, canceled, etc.

    -- Metricas financieras (aditivas — se pueden sumar)
    price                   DOUBLE NOT NULL,        -- Valor del item (revenue)
    freight_value           DOUBLE NOT NULL,        -- Costo de envio del item
    payment_value           DOUBLE,                 -- Pago total de la orden (denormalizado)

    -- Metrica de satisfaccion
    review_score            INTEGER,                -- 1-5 (denormalizado, por orden)

    -- Metricas de entrega (calculadas)
    order_purchase_date     DATE,                   -- Fecha de compra (sin hora)
    order_delivered_date    DATE,                    -- Fecha de entrega real
    order_estimated_date    DATE,                   -- Fecha de entrega estimada
    days_to_deliver         INTEGER,                -- Dias reales de entrega
    days_estimated          INTEGER,                -- Dias estimados de entrega
    delivery_delay_days     INTEGER,                -- Retraso en dias (+ = tarde, - = temprano)

    -- Constraint de PK compuesta
    PRIMARY KEY (order_id, order_item_id)
);
