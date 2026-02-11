{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ ref('olist_sellers_dataset') }}
),

geo AS (
    SELECT * FROM {{ ref('olist_geolocation_dataset') }}
)

SELECT
    s.seller_id,                                                        -- PK
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,

    g.geolocation_lat                           AS seller_lat,
    g.geolocation_lng                           AS seller_lng,

    -- Brazilian macro-regions (same logic as dim_customers)
    CASE s.seller_state
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
    END                                         AS seller_region,

    CURRENT_TIMESTAMP()                         AS dbt_loaded_at

FROM sellers s
LEFT JOIN geo g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix