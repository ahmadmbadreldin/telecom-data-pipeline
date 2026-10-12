{{ config(materialized='table') }}

SELECT
    HASH("incident_id") AS incident_key,
    "incident_id" AS incident_id,
    "severity" AS severity,
    "incident_status" AS incident_status,
    "root_cause" AS root_cause

FROM {{ source('telecom_staging', 'stg_network_incidents_reference') }}