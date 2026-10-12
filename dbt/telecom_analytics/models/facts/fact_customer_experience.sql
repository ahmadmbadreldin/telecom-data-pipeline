{{ config(materialized='table') }}

SELECT
    HASH(e."cx_id") AS cx_key,
    e."cx_id" AS cx_id,

    c.customer_key,
    p.plan_key,
    s.service_key,
    d.date_key,

    e."event_timestamp" AS event_timestamp,
    e."rating" AS rating,
    e."satisfaction_score" AS satisfaction_score,
    e."nps_score" AS nps_score,
    e."call_quality_score" AS call_quality_score,
    e."internet_experience_score" AS internet_experience_score,
    e."network_quality_score" AS network_quality_score,
    e."channel" AS channel

FROM {{ source('telecom_staging', 'stg_customer_experience') }} e

JOIN {{ ref('dim_customer') }} c
    ON e."customer_id" = c.customer_id

JOIN {{ source('telecom_staging', 'stg_customers') }} sc
    ON e."customer_id" = sc."customer_id"

JOIN {{ ref('dim_plan') }} p
    ON sc."plan_id" = p.plan_id

JOIN {{ ref('dim_service') }} s
    ON sc."service_id" = s.service_id

JOIN {{ ref('dim_date') }} d
    ON CAST(e."event_timestamp" AS DATE) = d.full_date