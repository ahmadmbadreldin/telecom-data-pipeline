{{ config(materialized='table') }}

SELECT
    HASH(t."tower_id") AS tower_key,
    t."tower_id" AS tower_id,
    t."tower_name" AS tower_name,

    a.area_key AS area_key,

    t."latitude" AS latitude,
    t."longitude" AS longitude,
    t."tower_type" AS tower_type,
    t."technology" AS technology,
    t."vendor" AS vendor,
    t."capacity" AS capacity,
    t."installation_date" AS installation_date,
    t."status" AS status

FROM {{ source('telecom_staging', 'stg_towers') }} AS t

JOIN {{ ref('dim_area') }} AS a
    ON t."area_id" = a.area_id