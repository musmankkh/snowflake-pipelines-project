{{ config(
    materialized = 'view',
    schema = 'analytics'
) }}

SELECT
    -- ── KEYS ─────────────────────────
    f.order_id,
    f.order_item_id,
    f.purchase_date,

    -- ── CUSTOMER DIMENSIONS ──────────
    c.customer_region,
    c.customer_state,

    -- ── PRODUCT DIMENSIONS ───────────
    p.product_category_name,

    -- ── SELLER DIMENSIONS ────────────
    s.seller_region,
    s.seller_state,

    -- ── ORDER ATTRIBUTES ─────────────
    f.order_status,
    f.review_sentiment,
    f.primary_payment_type,

    -- ── METRIC-READY FIELDS ──────────
    f.item_total_revenue,
    f.is_on_time,
    f.is_late,
    f.delivery_delta_days,

    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('fact_orders') }} f


LEFT JOIN {{ ref('dim_customers') }} c
    ON f.customer_id = c.customer_order_id   -- ✅ FIXED JOIN

LEFT JOIN {{ ref('dim_products') }} p
    ON f.product_id = p.product_id

LEFT JOIN {{ ref('dim_sellers') }} s
    ON f.seller_id = s.seller_id
