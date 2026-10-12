SELECT
    incident_key,
    customer_key,
    COUNT(*) AS duplicate_count
FROM {{ ref('fact_customer_incident_impact') }}
GROUP BY
    incident_key,
    customer_key
HAVING COUNT(*) > 1