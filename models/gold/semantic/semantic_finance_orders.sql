{{ config(
    materialized='view',
    schema='analytics'
) }}

SELECT
    -- ── DATE ───────────────────────
    purchase_date,

    -- ── ORDER ──────────────────────
    order_id,

    -- ── PAYMENT ────────────────────
    primary_payment_type,

    -- ── METRICS ────────────────────
    item_total_revenue,

    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('semantic_orders') }}
