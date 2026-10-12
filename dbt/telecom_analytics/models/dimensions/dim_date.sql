{{ config(materialized='table') }}

WITH dates AS (
    SELECT
        DATEADD(
            DAY,
            ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
            '2019-01-01'::DATE
        ) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 4383))
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    DAY(full_date) AS day,
    MONTH(full_date) AS month,
    MONTHNAME(full_date) AS month_name,
    QUARTER(full_date) AS quarter,
    YEAR(full_date) AS year,
    DAYOFWEEKISO(full_date) AS day_of_week,
    DAYOFWEEKISO(full_date) IN (5, 6) AS is_weekend

FROM dates