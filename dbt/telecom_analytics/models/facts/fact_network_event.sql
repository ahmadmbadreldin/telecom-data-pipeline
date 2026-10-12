{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_key'
) }}

WITH source_events AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY "event_id"
            ORDER BY "ingestion_time" DESC,
                     "kafka_timestamp" DESC,
                     "partition" DESC,
                     "offset" DESC
        ) AS rn

    FROM {{ source('telecom_staging', 'stg_network_events') }}

    WHERE "is_valid" = TRUE
      AND "event_id" IS NOT NULL

),

deduplicated_events AS (

    SELECT *
    FROM source_events
    WHERE rn = 1

)

SELECT
    HASH(e."event_id") AS event_key,
    e."event_id" AS event_id,

    t.tower_key,
    t.area_key,
    d.date_key,
    it.incident_type_key,

    e."event_time" AS event_timestamp,
    e."ingestion_time" AS ingestion_timestamp,

    e."signal_strength_dbm" AS signal_strength_dbm,
    e."call_drop_rate" AS call_drop_rate,
    e."latency_ms" AS latency_ms,
    e."download_speed_mbps" AS download_speed_mbps,
    e."upload_speed_mbps" AS upload_speed_mbps,
    e."packet_loss_pct" AS packet_loss_pct,
    e."network_status" AS network_status,

    e."ingestion_delay_minutes" AS ingestion_delay_minutes,

    e."topic" AS kafka_topic,
    e."partition" AS kafka_partition,
    e."offset" AS kafka_offset

FROM deduplicated_events e

JOIN {{ ref('dim_tower') }} t
    ON e."tower_id" = t.tower_id

JOIN {{ ref('dim_date') }} d
    ON CAST(e."event_time" AS DATE) = d.full_date

LEFT JOIN {{ ref('dim_incident_type') }} it
    ON e."incident_type_code" = it.incident_type_code