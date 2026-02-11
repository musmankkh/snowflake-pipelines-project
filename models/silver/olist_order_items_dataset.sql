{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        "order_id"             AS order_id,
        "order_item_id"        AS order_item_id,
        "product_id"           AS product_id,
        "seller_id"            AS seller_id,
        "shipping_limit_date"  AS shipping_limit_date,
        "price"                AS price,
        "freight_value"        AS freight_value
    FROM {{ source('bronze', 'olist_order_items_dataset') }}
),

converted_data AS (
    SELECT 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    FROM source_data
    WHERE order_id IS NOT NULL
      AND order_item_id IS NOT NULL
      AND TRY_CAST(order_item_id AS INTEGER) IS NOT NULL
      AND TRY_CAST(order_item_id AS INTEGER) >= 1
      AND product_id IS NOT NULL
      AND seller_id IS NOT NULL
      AND shipping_limit_date IS NOT NULL
),

deduplicated_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id
            ORDER BY shipping_limit_date DESC
        ) AS row_num
    FROM converted_data
)

SELECT 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
FROM deduplicated_data
WHERE row_num = 1