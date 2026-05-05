-- stg_order_items.sql — Modelo staging de items de ordenes

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    CAST(price AS DOUBLE)         AS price,
    CAST(freight_value AS DOUBLE) AS freight_value
FROM {{ source('staging', 'stg_order_items') }}
