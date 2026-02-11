{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('olist_orders_dataset') }}
),

items AS (
    SELECT * FROM {{ ref('olist_order_items_dataset') }}
),

payments AS (
    SELECT * FROM {{ ref('olist_order_payments_dataset') }}
),

reviews AS (
    SELECT * FROM {{ ref('olist_order_reviews_dataset') }}
),

-- ── Rank payments to determine primary payment method ──────────────────────
payments_ranked AS (
    SELECT
        order_id,
        payment_type,
        payment_value,
        payment_installments,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY payment_value DESC
        ) AS payment_rank
    FROM payments
),

primary_payment AS (
    SELECT
        order_id,
        payment_type AS primary_payment_type
    FROM payments_ranked
    WHERE payment_rank = 1
),

-- ── Aggregate payments to order level ──────────────────────────────────────
payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value)                                      AS total_payment_value,
        MAX(payment_installments)                               AS max_installments,
        MIN(payment_installments)                               AS min_installments,
        LISTAGG(DISTINCT payment_type, ', ')
            WITHIN GROUP (ORDER BY payment_type)                AS payment_types_used,
        COUNT(DISTINCT payment_type)                            AS num_payment_methods,
        IFF(COUNT(DISTINCT payment_type) > 1, TRUE, FALSE)     AS is_split_payment
    FROM payments
    GROUP BY order_id
),

payments_final AS (
    SELECT
        a.*,
        p.primary_payment_type
    FROM payments_agg a
    LEFT JOIN primary_payment p
        ON a.order_id = p.order_id
),

-- ── Aggregate reviews to order level ───────────────────────────────────────
reviews_agg AS (
    SELECT
        order_id,
        MAX(review_score)                                       AS review_score,
        COUNT(review_id)                                        AS review_count,
        MAX(review_creation_date)                               AS review_creation_date,
        IFF(
            COUNT(CASE WHEN review_comment_message IS NOT NULL THEN 1 END) > 0,
            TRUE, FALSE
        )                                                       AS has_review_comment
    FROM reviews
    GROUP BY order_id
),

-- ── Join everything at ORDER_ITEM grain ────────────────────────────────────
final AS (
    SELECT
        -- ── KEYS ───────────────────────────────────────────────────────────
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,
        o.customer_id,

        -- ── DATE KEYS ──────────────────────────────────────────────────────
        o.order_purchase_timestamp::DATE        AS purchase_date,
        o.order_approved_at::DATE               AS approved_date,
        o.order_delivered_carrier_date::DATE    AS carrier_date,
        o.order_delivered_customer_date::DATE   AS delivered_date,
        o.order_estimated_delivery_date::DATE   AS estimated_delivery_date,

        -- ── ORDER ATTRIBUTES ───────────────────────────────────────────────
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- ── ITEM FINANCIALS ────────────────────────────────────────────────
        i.price                                 AS item_price,
        i.freight_value                         AS item_freight,
        i.price + i.freight_value               AS item_total_revenue,
        i.shipping_limit_date,

        -- ── PAYMENT FACTS ──────────────────────────────────────────────────
        p.total_payment_value,
        p.max_installments,
        p.min_installments,
        p.payment_types_used,
        p.num_payment_methods,
        p.is_split_payment,
        p.primary_payment_type,

        -- ── REVIEW FACTS ───────────────────────────────────────────────────
        r.review_score,
        r.review_count,
        r.review_creation_date,
        r.has_review_comment,

        -- ── DELIVERY METRICS ───────────────────────────────────────────────
        DATEDIFF('day', o.order_purchase_timestamp, o.order_approved_at)
            AS approval_time_days,

        DATEDIFF('day', o.order_purchase_timestamp, o.order_delivered_customer_date)
            AS actual_delivery_days,

        DATEDIFF('day', o.order_purchase_timestamp, o.order_estimated_delivery_date)
            AS estimated_delivery_days,

        DATEDIFF('day',
            o.order_estimated_delivery_date,
            o.order_delivered_customer_date)
            AS delivery_delta_days,

        IFF(
            o.order_delivered_customer_date <= o.order_estimated_delivery_date,
            TRUE, FALSE
        ) AS is_on_time,

        IFF(
            o.order_delivered_customer_date > o.order_estimated_delivery_date,
            TRUE, FALSE
        ) AS is_late,

        -- ── REVIEW SENTIMENT ───────────────────────────────────────────────
        CASE
            WHEN r.review_score >= 4 THEN 'positive'
            WHEN r.review_score =  3 THEN 'neutral'
            WHEN r.review_score <= 2 THEN 'negative'
            ELSE 'no_review'
        END AS review_sentiment,

        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at

    FROM items i
    INNER JOIN orders        o ON i.order_id = o.order_id
    LEFT  JOIN payments_final p ON i.order_id = p.order_id
    LEFT  JOIN reviews_agg   r ON i.order_id = r.order_id
)

SELECT * FROM final