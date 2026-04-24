-- stg_sellers.sql — Modelo staging de vendedores

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('staging', 'stg_sellers') }}
