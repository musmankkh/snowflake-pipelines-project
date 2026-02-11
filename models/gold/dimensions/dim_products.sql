{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('olist_products_dataset') }}
)

SELECT
    product_id,                                                         -- PK
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    is_uncategorized,
    has_outlier_dimensions,

    -- Business weight classification
    CASE
        WHEN product_weight_g <   300  THEN 'light'
        WHEN product_weight_g <  2000  THEN 'medium'
        WHEN product_weight_g < 10000  THEN 'heavy'
        ELSE                                'very_heavy'
    END                                                                 AS weight_class,

    -- Photo richness tier
    CASE
        WHEN product_photos_qty = 0   THEN 'no_photo'
        WHEN product_photos_qty = 1   THEN 'single_photo'
        WHEN product_photos_qty <= 3  THEN 'few_photos'
        ELSE                               'many_photos'
    END                                                                 AS photo_tier,

    -- Density proxy (g per cm³) — higher = compact/dense item
    ROUND(
        product_weight_g / NULLIF(product_volume_cm3, 0),
        4
    )                                                                   AS density_g_per_cm3,

    CURRENT_TIMESTAMP()                                                 AS dbt_loaded_at

FROM products