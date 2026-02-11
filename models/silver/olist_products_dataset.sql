{{ config(materialized = 'table') }}

WITH source_products AS (
    SELECT
        "product_id"                     AS product_id,
        "product_category_name"          AS product_category_name,
        "product_name_lenght"            AS product_name_length,
        "product_description_lenght"     AS product_description_length,
        "product_photos_qty"             AS product_photos_qty,
        "product_weight_g"               AS product_weight_g,
        "product_length_cm"              AS product_length_cm,
        "product_height_cm"              AS product_height_cm,
        "product_width_cm"               AS product_width_cm
    FROM {{ source('bronze', 'olist_products_dataset') }}
),

source_translations AS (
    SELECT
        "product_category_name"          AS product_category_name,
        "product_category_name_english"  AS product_category_name_english
    FROM {{ source('bronze', 'product_category_name_translation') }}
),

deduped_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY product_id
        ) AS row_num
    FROM source_products
    WHERE product_id IS NOT NULL
),

products_joined AS (
    SELECT
        p.product_id,
        COALESCE(
            t.product_category_name_english,
            p.product_category_name,
            'uncategorized'
        ) AS product_category_name,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM deduped_products p
    LEFT JOIN source_translations t
        ON LOWER(TRIM(p.product_category_name))
         = LOWER(TRIM(t.product_category_name))
    WHERE p.row_num = 1
),

clamped AS (
    SELECT
        product_id,
        product_category_name,
        CAST(product_name_length AS INTEGER)         AS product_name_length,
        CAST(product_description_length AS INTEGER)  AS product_description_length,
        COALESCE(product_photos_qty, 0)              AS product_photos_qty,

        CAST(
            CASE
                WHEN product_weight_g < 10 THEN 10
                WHEN product_weight_g > 50000 THEN 50000
                ELSE product_weight_g
            END AS NUMBER(10,2)
        ) AS product_weight_g,

        CAST(
            CASE
                WHEN product_length_cm <= 0 THEN 1
                WHEN product_length_cm > 200 THEN 200
                ELSE product_length_cm
            END AS NUMBER(10,2)
        ) AS product_length_cm,

        CAST(
            CASE
                WHEN product_height_cm <= 0 THEN 1
                WHEN product_height_cm > 200 THEN 200
                ELSE product_height_cm
            END AS NUMBER(10,2)
        ) AS product_height_cm,

        CAST(
            CASE
                WHEN product_width_cm <= 0 THEN 1
                WHEN product_width_cm > 200 THEN 200
                ELSE product_width_cm
            END AS NUMBER(10,2)
        ) AS product_width_cm,

        product_weight_g  AS raw_weight_g,
        product_length_cm AS raw_length_cm,
        product_height_cm AS raw_height_cm,
        product_width_cm  AS raw_width_cm
    FROM products_joined
),

final AS (
    SELECT
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,

        (product_length_cm * product_height_cm * product_width_cm)
            AS product_volume_cm3,

        product_category_name = 'uncategorized'
            AS is_uncategorized,

        (
            raw_weight_g < 10 OR raw_weight_g > 50000
            OR raw_length_cm <= 0 OR raw_length_cm > 200
            OR raw_height_cm <= 0 OR raw_height_cm > 200
            OR raw_width_cm  <= 0 OR raw_width_cm  > 200
        ) AS has_outlier_dimensions,

        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
    FROM clamped
)

SELECT * FROM final