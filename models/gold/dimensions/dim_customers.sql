{{ config(
    materialized = 'table'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('olist_customers_dataset') }}
),

geo AS (
    SELECT * FROM {{ ref('olist_geolocation_dataset') }}
),


deduped AS (
    SELECT
        customer_unique_id,
        customer_id                         AS customer_order_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY customer_id
        ) AS rn
    FROM customers
),

enriched AS (
    SELECT
        d.customer_unique_id               AS customer_id,        -- DIMENSION PK
        d.customer_order_id,                                   -- FACT FK
        d.customer_zip_code_prefix,
        d.customer_city,
        d.customer_state,
        g.geolocation_lat                  AS customer_lat,
        g.geolocation_lng                  AS customer_lng,

        -- Brazilian macro-regions
        CASE d.customer_state
            WHEN 'SP' THEN 'Southeast'   WHEN 'RJ' THEN 'Southeast'
            WHEN 'MG' THEN 'Southeast'   WHEN 'ES' THEN 'Southeast'
            WHEN 'PR' THEN 'South'       WHEN 'SC' THEN 'South'
            WHEN 'RS' THEN 'South'
            WHEN 'BA' THEN 'Northeast'   WHEN 'PE' THEN 'Northeast'
            WHEN 'CE' THEN 'Northeast'   WHEN 'MA' THEN 'Northeast'
            WHEN 'PB' THEN 'Northeast'   WHEN 'RN' THEN 'Northeast'
            WHEN 'PI' THEN 'Northeast'   WHEN 'AL' THEN 'Northeast'
            WHEN 'SE' THEN 'Northeast'
            WHEN 'AM' THEN 'North'       WHEN 'PA' THEN 'North'
            WHEN 'RO' THEN 'North'       WHEN 'RR' THEN 'North'
            WHEN 'AC' THEN 'North'       WHEN 'AP' THEN 'North'
            WHEN 'TO' THEN 'North'
            WHEN 'MT' THEN 'Center-West' WHEN 'MS' THEN 'Center-West'
            WHEN 'GO' THEN 'Center-West' WHEN 'DF' THEN 'Center-West'
            ELSE 'Unknown'
        END                                AS customer_region

    FROM deduped d
    LEFT JOIN geo g
        ON d.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    WHERE d.rn = 1
)

SELECT
    customer_id,
    customer_order_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    customer_region,
    customer_lat,
    customer_lng,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM enriched