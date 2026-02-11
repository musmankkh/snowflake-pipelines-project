{{ config(materialized='table') }}

WITH source_data AS (
  SELECT
    "customer_id"                AS customer_id,
    "customer_unique_id"         AS customer_unique_id,
    "customer_zip_code_prefix"   AS customer_zip_code_prefix,
    "customer_city"              AS customer_city,
    "customer_state"             AS customer_state
  FROM {{ source('bronze', 'olist_customers_dataset') }}
),

-- 1. BASIC CLEANING + EXPLICIT TYPE CASTING
standardized AS (
  SELECT
    CAST(customer_id AS VARCHAR)                AS customer_id,
    CAST(customer_unique_id AS VARCHAR)         AS customer_unique_id,

    -- Force ZIP as text for BI tools
    CAST(customer_zip_code_prefix AS VARCHAR)   AS customer_zip_code_prefix,

    -- Normalize and force VARCHAR
    CAST(TRIM(LOWER(customer_city)) AS VARCHAR) AS customer_city_clean,
    CAST(TRIM(UPPER(customer_state)) AS VARCHAR) AS customer_state_clean
  FROM source_data
  WHERE customer_id IS NOT NULL
    AND customer_unique_id IS NOT NULL
),

-- 2. REMOVE DUPLICATES BASED ON CUSTOMER_ID
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY customer_unique_id
    ) AS row_num
  FROM standardized
),

-- 3. FINAL OUTPUT
final AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city_clean   AS customer_city,
    customer_state_clean  AS customer_state,

    -- Explicit Snowflake timestamp for BI tools
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ) AS dbt_loaded_at
  FROM deduped
  WHERE row_num = 1
)

SELECT * FROM final