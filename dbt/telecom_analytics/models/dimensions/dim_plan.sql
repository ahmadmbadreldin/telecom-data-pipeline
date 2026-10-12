{{ config(materialized='table') }}

SELECT
    HASH("plan_id") AS plan_key,
    "plan_id" AS plan_id,
    "plan_name" AS plan_name,
    "plan_type" AS plan_type,
    "monthly_fee" AS monthly_fee,
    "data_limit_gb" AS data_limit_gb,
    "voice_minutes" AS voice_minutes,
    "sms_limit" AS sms_limit,
    "speed_limit_mbps" AS speed_limit_mbps,
    "effective_from" AS effective_from,
    "effective_to" AS effective_to,
    "is_active" AS is_active,
    "is_unlimited_data" AS is_unlimited_data

FROM {{ source('telecom_staging', 'stg_plans') }}