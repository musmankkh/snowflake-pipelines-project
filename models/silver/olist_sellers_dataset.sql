{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT     
        "seller_id"              AS seller_id,
        "seller_zip_code_prefix" AS seller_zip_code_prefix,
        "seller_city"            AS seller_city,
        "seller_state"           AS seller_state
    FROM {{ source('bronze', 'olist_sellers_dataset') }}
),

-- STEP 1: Basic Cleaning & Type Conversion
basic_cleaning AS (
    SELECT
        TRIM(seller_id) AS seller_id,

        -- ZIP as string with leading zeros
        LPAD(CAST(seller_zip_code_prefix AS VARCHAR), 5, '0')
            AS seller_zip_code_prefix,

        -- Normalize city
        LOWER(
            REGEXP_REPLACE(
                TRIM(seller_city),
                '\s+', ' '
            )
        ) AS seller_city_normalized,

        -- Normalize state
        UPPER(TRIM(seller_state)) AS seller_state_normalized
    FROM source_data
    WHERE seller_id IS NOT NULL 
      AND seller_zip_code_prefix IS NOT NULL
      AND seller_city IS NOT NULL
      AND seller_state IS NOT NULL
),

-- STEP 2: Remove duplicate sellers
deduplicate_sellers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY seller_zip_code_prefix
        ) AS seller_id_row_num
    FROM basic_cleaning
),

-- STEP 3: Remove duplicate ZIPs (keep first seller per ZIP)
deduplicate_locations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_zip_code_prefix
            ORDER BY seller_id
        ) AS zip_row_num
    FROM deduplicate_sellers
    WHERE seller_id_row_num = 1
),

-- STEP 4: Data Quality Checks
quality_checks AS (
    SELECT
        *,

        CASE 
            WHEN LENGTH(seller_zip_code_prefix) != 5 THEN TRUE
            WHEN CAST(seller_zip_code_prefix AS INTEGER) < 1000 THEN TRUE
            WHEN CAST(seller_zip_code_prefix AS INTEGER) > 99999 THEN TRUE
            ELSE FALSE
        END AS is_invalid_zip,

        CASE 
            WHEN LENGTH(seller_state_normalized) != 2 THEN TRUE
            WHEN seller_state_normalized NOT IN (
                'AC','AL','AP','AM','BA','CE','DF','ES','GO',
                'MA','MT','MS','MG','PA','PB','PR','PE','PI',
                'RJ','RN','RS','RO','RR','SC','SP','SE','TO'
            ) THEN TRUE
            ELSE FALSE
        END AS is_invalid_state,

        CASE 
            WHEN TRIM(seller_id) = '' THEN TRUE
            WHEN TRIM(seller_city_normalized) = '' THEN TRUE
            ELSE FALSE
        END AS is_empty_string
    FROM deduplicate_locations
    WHERE zip_row_num = 1
),

-- STEP 5: Final Clean Dataset
final AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_state_normalized AS seller_state,
        seller_city_normalized  AS seller_city,
        CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
    FROM quality_checks
    WHERE NOT (is_invalid_zip OR is_invalid_state OR is_empty_string)
)

SELECT * FROM final