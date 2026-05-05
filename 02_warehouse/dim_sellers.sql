-- ============================================================================
-- dim_sellers.sql — Poblar la dimension de vendedores
-- ============================================================================

INSERT INTO warehouse.dim_sellers
SELECT
    ROW_NUMBER() OVER (ORDER BY seller_id) AS seller_key,
    seller_id,
    seller_city,
    seller_state
FROM staging.stg_sellers;
