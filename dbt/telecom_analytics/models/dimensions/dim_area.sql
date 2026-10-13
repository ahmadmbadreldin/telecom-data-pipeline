{{ config(materialized='table') }}

SELECT
    HASH("area_id") AS area_key,
    "area_id" AS area_id,
    "area_name" AS area_name,
    "city" AS city,
    "governorate" AS governorate,
    "region" AS region,
    "country" AS country,
    "latitude" AS latitude,
    "longitude" AS longitude,
    "population_density" AS population_density

FROM {{ source('telecom_staging', 'stg_areas') }}