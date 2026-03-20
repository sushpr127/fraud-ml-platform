{{ config(materialized='view') }}

SELECT
    RECORD_CONTENT:transaction_id::VARCHAR   AS transaction_id,
    RECORD_CONTENT:user_id::VARCHAR          AS user_id,
    RECORD_CONTENT:merchant_id::VARCHAR      AS merchant_id,
    RECORD_CONTENT:amount::FLOAT             AS amount,
    RECORD_CONTENT:timestamp::INTEGER        AS transaction_ts,
    RECORD_CONTENT:event_time::TIMESTAMP_NTZ AS event_time,
    RECORD_CONTENT:card_type::VARCHAR        AS card_type,
    RECORD_CONTENT:email_domain::VARCHAR     AS email_domain,
    RECORD_CONTENT:is_fraud::INTEGER         AS is_fraud,
    SYSDATE()                                AS loaded_at
FROM {{ source('raw', 'RAW_TRANSACTIONS') }}
WHERE RECORD_CONTENT IS NOT NULL
