{{ config(materialized='table') }}

SELECT
    HASH("service_id") AS service_key,
    "service_id" AS service_id,
    "service_name" AS service_name,
    "service_category" AS service_category,
    "technology" AS technology,
    "description" AS description,
    "is_active" AS is_active

FROM {{ source('telecom_staging', 'stg_services') }}
