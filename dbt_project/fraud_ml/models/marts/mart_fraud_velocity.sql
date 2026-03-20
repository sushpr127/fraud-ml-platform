{{ config(materialized='table') }}

SELECT
    HOUR(event_time)                     AS hour_of_day,
    COUNT(*)                             AS total_transactions,
    SUM(is_fraud)                        AS fraud_count,
    SUM(is_fraud) / COUNT(*)::FLOAT      AS fraud_rate,
    AVG(amount)                          AS avg_amount
FROM {{ ref('stg_transactions') }}
GROUP BY 1
ORDER BY 1
