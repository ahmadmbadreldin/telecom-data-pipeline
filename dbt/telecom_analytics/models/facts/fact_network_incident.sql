{{ config(materialized='table') }}

SELECT
    i.incident_key,
    t.tower_key,
    it.incident_type_key,
    d.date_key,
    a.area_key,

    n."start_timestamp" AS start_timestamp,
    n."end_timestamp" AS end_timestamp,

    n."duration_minutes" AS duration_minutes,
    n."max_latency_ms" AS max_latency_ms,
    n."avg_latency_ms" AS avg_latency_ms,
    n."avg_signal_strength_dbm" AS avg_signal_strength_dbm,
    n."max_call_drop_rate" AS max_call_drop_rate,
    n."avg_download_speed_mbps" AS avg_download_speed_mbps,

   n."affected_customer_count" AS affected_customers_count,
    n."complaint_count" AS complaint_count

FROM {{ source('telecom_staging', 'stg_network_incidents_reference') }} n

JOIN {{ ref('dim_incident') }} i
    ON n."incident_id" = i.incident_id

JOIN {{ ref('dim_tower') }} t
    ON n."tower_id" = t.tower_id

JOIN {{ ref('dim_incident_type') }} it
    ON n."incident_type_id" = it.incident_type_id

JOIN {{ ref('dim_date') }} d
    ON CAST(n."start_timestamp" AS DATE) = d.full_date

JOIN {{ ref('dim_area') }} a
    ON n."area_id" = a.area_id