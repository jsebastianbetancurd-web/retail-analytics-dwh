-- stg_customers.sql — Modelo staging de clientes

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('staging', 'stg_customers') }}
