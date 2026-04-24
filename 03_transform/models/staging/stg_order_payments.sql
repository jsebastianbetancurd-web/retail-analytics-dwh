-- stg_order_payments.sql — Modelo staging de pagos

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS DOUBLE) AS payment_value
FROM {{ source('staging', 'stg_order_payments') }}
