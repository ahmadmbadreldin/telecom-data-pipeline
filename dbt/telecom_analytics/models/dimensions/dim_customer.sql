{{ config(materialized='table') }}

SELECT
    HASH(c."customer_id") AS customer_key,
    c."customer_id" AS customer_id,

    a.area_key AS area_key,

    c."customer_name" AS customer_name,
    c."age" AS age,
    c."age_group" AS age_group,
    c."customer_segment" AS segment,
    c."subscription_status" AS subscription_status,
    c."activation_date" AS activation_date,
    c."churn_date" AS churn_date,
    c."last_updated_at" AS last_updated_at

FROM {{ source('telecom_staging', 'stg_customers') }} AS c

JOIN {{ ref('dim_area') }} AS a
    ON c."area_id" = a.area_id