{{ config(materialized='table') }}

SELECT
    merchant_id,
    DATE_TRUNC('day', event_time)        AS transaction_date,
    COUNT(*)                             AS total_transactions,
    SUM(is_fraud)                        AS fraud_count,
    SUM(is_fraud) / COUNT(*)::FLOAT      AS fraud_rate,
    AVG(amount)                          AS avg_transaction_amount,
    SUM(amount)                          AS total_volume
FROM {{ ref('stg_transactions') }}
GROUP BY 1, 2
ORDER BY fraud_rate DESC
