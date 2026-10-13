{{ config(materialized='table') }}

SELECT
    HASH("complaint_type_id") AS complaint_type_key,
    "complaint_type_id" AS complaint_type_id,
    "complaint_code" AS complaint_code,
    "complaint_name" AS complaint_name,
    "category" AS category,
    "default_priority" AS default_priority,
    "description" AS description

FROM {{ source('telecom_staging', 'stg_complaint_types') }}