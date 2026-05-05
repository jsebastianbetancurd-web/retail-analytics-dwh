-- stg_order_reviews.sql — Modelo staging de resenas

SELECT
    review_id,
    order_id,
    CAST(review_score AS INTEGER) AS review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM {{ source('staging', 'stg_order_reviews') }}
