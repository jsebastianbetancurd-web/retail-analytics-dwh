-- stg_category_translation.sql — Traduccion de categorias

SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('staging', 'stg_category_translation') }}
