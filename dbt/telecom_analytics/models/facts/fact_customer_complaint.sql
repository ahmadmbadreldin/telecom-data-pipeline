{{ config(materialized='table') }}

SELECT
    HASH(s."complaint_id") AS complaint_key,
    s."complaint_id" AS complaint_id,

    c.customer_key,
    ct.complaint_type_key,
    i.incident_key,

    dc.date_key AS created_date_key,
    dr.date_key AS resolved_date_key,

    s."created_timestamp" AS created_timestamp,
    s."resolved_timestamp" AS resolved_timestamp,

    s."status" AS status,
    s."priority" AS priority,
    s."channel" AS channel,
    s."complaint_text" AS complaint_text,

    s."resolution_minutes" AS resolution_minutes,
    s."sla_breached" AS sla_breached

FROM {{ source('telecom_staging', 'stg_complaints') }} s

JOIN {{ ref('dim_customer') }} c
    ON s."customer_id" = c.customer_id

JOIN {{ ref('dim_complaint_type') }} ct
    ON s."complaint_type_id" = ct.complaint_type_id

LEFT JOIN {{ ref('dim_incident') }} i
    ON s."incident_id" = i.incident_id

JOIN {{ ref('dim_date') }} dc
    ON CAST(s."created_timestamp" AS DATE) = dc.full_date

LEFT JOIN {{ ref('dim_date') }} dr
    ON CAST(s."resolved_timestamp" AS DATE) = dr.full_date