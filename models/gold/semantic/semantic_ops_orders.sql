{{ config(
    materialized='view',
    schema='analytics'
) }}

SELECT
    -- ── DATE ───────────────────────
    purchase_date,

    -- ── SELLER ─────────────────────
    seller_region,
    seller_state,

    -- ── DELIVERY METRICS ───────────
    is_on_time,
    is_late,
    delivery_delta_days,

    CURRENT_TIMESTAMP() AS dbt_loaded_at

FROM {{ ref('semantic_orders') }}
