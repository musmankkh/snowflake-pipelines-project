{{ config(
    materialized = 'table'
) }}

WITH source_data AS (
    SELECT 
        "review_id"               AS review_id,
        "order_id"                AS order_id,
        "review_score"            AS review_score,
        "review_comment_title"    AS review_comment_title,
        "review_comment_message"  AS review_comment_message,
        "review_creation_date"    AS review_creation_date,
        "review_answer_timestamp" AS review_answer_timestamp
    FROM {{ source('bronze', 'olist_order_reviews_dataset') }}
),

validated_data AS (
    SELECT *
    FROM source_data
    WHERE review_id IS NOT NULL
      AND order_id IS NOT NULL
      AND review_score BETWEEN 1 AND 5
      AND review_creation_date IS NOT NULL
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY review_creation_date DESC,
                     review_answer_timestamp DESC
        ) AS row_num
    FROM validated_data
),

final AS (
    SELECT
        review_id,
        order_id,
        CAST(review_score AS INTEGER) AS review_score,

        CASE 
            WHEN review_comment_title IS NULL OR TRIM(review_comment_title) = '' THEN NULL
            WHEN LOWER(TRIM(review_comment_title)) IN ('na','n/a','null','none') THEN NULL
            ELSE TRIM(review_comment_title)
        END AS review_comment_title,

        CASE 
            WHEN review_comment_message IS NULL OR TRIM(review_comment_message) = '' THEN NULL
            WHEN LOWER(TRIM(review_comment_message)) IN ('na','n/a','null','none') THEN NULL
            ELSE TRIM(review_comment_message)
        END AS review_comment_message,

        review_creation_date,
        review_answer_timestamp,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
    FROM deduplicated_data
    WHERE row_num = 1
)

SELECT * FROM final