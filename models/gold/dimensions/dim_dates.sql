{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT
        DATEADD('day', seq4(), '2016-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3652))  -- ~10 years
    WHERE date_day <= CURRENT_DATE()
)

SELECT
    date_day                                                AS date_id,
    DATE_PART('year',    date_day)::INTEGER                 AS year,
    DATE_PART('month',   date_day)::INTEGER                 AS month,
    DATE_PART('day',     date_day)::INTEGER                 AS day,
    DATE_PART('week',    date_day)::INTEGER                 AS week_of_year,
    DATE_PART('quarter', date_day)::INTEGER                 AS quarter,
    TO_CHAR(date_day, 'YYYY-MM')                            AS year_month,
    TO_CHAR(date_day, 'YYYY-"Q"Q')                          AS year_quarter,
    DAYNAME(date_day)                                       AS day_name,
    MONTHNAME(date_day)                                     AS month_name,
    IFF(DAYOFWEEKISO(date_day) IN (6, 7), TRUE, FALSE)      AS is_weekend,
    CASE DATE_PART('quarter', date_day)
        WHEN 1 THEN 'Q1 (Jan-Mar)'
        WHEN 2 THEN 'Q2 (Apr-Jun)'
        WHEN 3 THEN 'Q3 (Jul-Sep)'
        WHEN 4 THEN 'Q4 (Oct-Dec)'
    END                                                     AS quarter_label,
    CURRENT_TIMESTAMP()                                     AS dbt_loaded_at

FROM date_spine