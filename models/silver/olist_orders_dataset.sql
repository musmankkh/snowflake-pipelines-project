{{ config(materialized = 'table') }}

WITH source_data AS (
    SELECT 
        "order_id"                      AS order_id,
        "customer_id"                   AS customer_id,
        "order_status"                  AS order_status,

        TRY_TO_TIMESTAMP_NTZ("order_purchase_timestamp", 'MM/DD/YYYY HH24:MI')
            AS order_purchase_timestamp,

        TRY_TO_TIMESTAMP_NTZ("order_approved_at", 'MM/DD/YYYY HH24:MI')
            AS order_approved_at,

        TRY_TO_TIMESTAMP_NTZ("order_delivered_carrier_date", 'MM/DD/YYYY HH24:MI')
            AS order_delivered_carrier_date,

        TRY_TO_TIMESTAMP_NTZ("order_delivered_customer_date", 'MM/DD/YYYY HH24:MI')
            AS order_delivered_customer_date,

        TRY_TO_TIMESTAMP_NTZ("order_estimated_delivery_date", 'MM/DD/YYYY HH24:MI')
            AS order_estimated_delivery_date
    FROM {{ source('bronze', 'olist_orders_dataset') }}
),

filtered AS (
    SELECT *
    FROM source_data
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_status IS NOT NULL
      AND order_purchase_timestamp IS NOT NULL
      AND order_approved_at IS NOT NULL
      AND order_delivered_carrier_date IS NOT NULL
      AND order_delivered_customer_date IS NOT NULL
      AND order_estimated_delivery_date IS NOT NULL
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_purchase_timestamp DESC
        ) AS row_num
    FROM filtered
),

time_deltas AS (
    SELECT
        *,
        DATEDIFF(
            'day',
            order_purchase_timestamp,
            order_delivered_customer_date
        ) AS delivery_time_days
    FROM deduped
    WHERE row_num = 1
),

delivery_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY delivery_time_days) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY delivery_time_days) AS q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY delivery_time_days)
        - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY delivery_time_days) AS iqr
    FROM time_deltas
),

validated AS (
    SELECT
        td.*,

        order_delivered_customer_date < order_purchase_timestamp
            AS is_delivery_before_purchase,

        order_delivered_customer_date < order_delivered_carrier_date
            AS is_delivery_before_carrier,

        order_approved_at < order_purchase_timestamp
            AS is_approval_before_purchase,

        order_estimated_delivery_date < order_purchase_timestamp
            AS is_estimated_before_purchase,

        delivery_time_days < (ds.q1 - 1.5 * ds.iqr)
            OR delivery_time_days > (ds.q3 + 1.5 * ds.iqr)
            AS is_delivery_time_outlier

    FROM time_deltas td
    CROSS JOIN delivery_stats ds
),

final AS (
    SELECT
        order_id,
        customer_id,
        UPPER(TRIM(order_status)) AS order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
    FROM validated
    WHERE NOT is_delivery_before_purchase
      AND NOT is_delivery_before_carrier
      AND NOT is_approval_before_purchase
      AND NOT is_estimated_before_purchase
      AND NOT is_delivery_time_outlier
)

SELECT * FROM final