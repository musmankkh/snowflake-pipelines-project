{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        "order_id"               AS order_id,
        "payment_sequential"     AS payment_sequential,
        "payment_installments"   AS payment_installments,
        "payment_type"           AS payment_type,
        "payment_value"          AS payment_value
    FROM {{ source('bronze', 'olist_order_payments_dataset') }}
),

/* 1. Remove null keys + dedupe (keep highest payment_value per combo) */
cleaned_data AS (
    SELECT
        sd.*,
        ROW_NUMBER() OVER (
            PARTITION BY sd.order_id, sd.payment_sequential
            ORDER BY sd.payment_value DESC NULLS LAST
        ) AS row_num
    FROM source_data sd
    WHERE sd.order_id IS NOT NULL
      AND sd.payment_sequential IS NOT NULL
),

/* 2. Normalize types + safe casting (Snowflake) */
casted_data AS (
    SELECT
        order_id,
        TRY_CAST(payment_sequential AS INTEGER) AS payment_sequential,
        CASE
            WHEN LOWER(TRIM(payment_type)) IN ('credit_card','credit card','card') THEN 'credit_card'
            WHEN LOWER(TRIM(payment_type)) IN ('boleto','bank_slip','bank slip') THEN 'boleto'
            WHEN LOWER(TRIM(payment_type)) IN ('voucher','gift_card') THEN 'voucher'
            WHEN LOWER(TRIM(payment_type)) IN ('debit_card','debit card') THEN 'debit_card'
            WHEN LOWER(TRIM(payment_type)) IN ('not_defined','undefined','unknown') THEN 'not_defined'
            ELSE LOWER(TRIM(payment_type))
        END AS payment_type,
        TRY_CAST(payment_installments AS INTEGER) AS payment_installments,
        CAST(ROUND(payment_value, 2) AS NUMBER(10,2)) AS payment_value
    FROM cleaned_data
    WHERE row_num = 1
),

/* 3. Compute outlier bounds for payment_value (IQR) */
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY payment_value) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY payment_value) AS q3
    FROM casted_data
    WHERE payment_value IS NOT NULL
),

bounds AS (
    SELECT
        q1,
        q3,
        (q3 + 1.5 * (q3 - q1)) AS upper_bound,
        (q1 - 1.5 * (q3 - q1)) AS lower_bound
    FROM percentiles
),

/* 4. Final filtered output */
final AS (
    SELECT
        c.order_id,
        c.payment_sequential,
        c.payment_type,
        c.payment_installments,
        c.payment_value,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
    FROM casted_data c
    CROSS JOIN bounds b
    WHERE c.payment_installments > 0
      AND c.payment_value > 0
      AND c.payment_value BETWEEN b.lower_bound AND b.upper_bound
)

SELECT *
FROM final