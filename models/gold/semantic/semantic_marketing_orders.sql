{{ config(
    materialized='view',
    schema='analytics'
) }}

SELECT
    -- ── DATE ───────────────────────
    purchase_date,

    -- ── CUSTOMER ───────────────────
    customer_region,
    customer_state,

    -- ── PRODUCT ────────────────────
    product_category_name,

    -- ── METRICS ────────────────────
    item_total_revenue,

    -- ── FEEDBACK ───────────────────
    review_sentiment,

    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('semantic_orders') }}
