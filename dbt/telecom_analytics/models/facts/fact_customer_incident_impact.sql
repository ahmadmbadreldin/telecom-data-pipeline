{{ config(materialized='table') }}

SELECT
    i.incident_key,
    c.customer_key,

    ds.date_key AS impact_start_date_key,
    de.date_key AS impact_end_date_key,

    b."impact_start_timestamp" AS impact_start_timestamp,
    b."impact_end_timestamp" AS impact_end_timestamp,
    b."impact_duration_minutes" AS impact_duration_minutes

FROM {{ source('telecom_staging', 'stg_bridge_incident_customer') }} b

JOIN {{ ref('dim_incident') }} i
    ON b."incident_id" = i.incident_id

JOIN {{ ref('dim_customer') }} c
    ON b."customer_id" = c.customer_id

JOIN {{ ref('dim_date') }} ds
    ON CAST(b."impact_start_timestamp" AS DATE) = ds.full_date

JOIN {{ ref('dim_date') }} de
    ON CAST(b."impact_end_timestamp" AS DATE) = de.full_date