{{ config(materialized='table') }}

SELECT
    HASH("incident_type_id") AS incident_type_key,
    "incident_type_id" AS incident_type_id,
    "incident_type_code" AS incident_type_code,
    "incident_type_name" AS incident_type_name,
    "category" AS category,
    "default_severity" AS default_severity,
    "description" AS description

FROM {{ source('telecom_staging', 'stg_incident_types') }}