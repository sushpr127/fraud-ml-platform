{{ config(materialized='table') }}

WITH baseline AS (
    SELECT
        WIDTH_BUCKET(amount, 0, 2000, 10) AS bucket,
        COUNT(*) AS baseline_count
    FROM {{ ref('stg_transactions') }}
    WHERE event_time < DATEADD('day', -7, SYSDATE())
    GROUP BY 1
),

recent AS (
    SELECT
        WIDTH_BUCKET(amount, 0, 2000, 10) AS bucket,
        COUNT(*) AS recent_count
    FROM {{ ref('stg_transactions') }}
    WHERE event_time >= DATEADD('day', -7, SYSDATE())
    GROUP BY 1
),

psi_calc AS (
    SELECT
        b.bucket,
        b.baseline_count / SUM(b.baseline_count) OVER () AS baseline_pct,
        r.recent_count   / SUM(r.recent_count)   OVER () AS recent_pct
    FROM baseline b
    JOIN recent r ON b.bucket = r.bucket
)

SELECT
    SYSDATE()                                            AS calculated_at,
    SUM(
        (recent_pct - baseline_pct) *
        LN(recent_pct / NULLIF(baseline_pct, 0))
    )                                                    AS psi_score,
    CASE
        WHEN SUM(
            (recent_pct - baseline_pct) *
            LN(recent_pct / NULLIF(baseline_pct, 0))
        ) < 0.1  THEN 'STABLE'
        WHEN SUM(
            (recent_pct - baseline_pct) *
            LN(recent_pct / NULLIF(baseline_pct, 0))
        ) < 0.2  THEN 'MONITOR'
        ELSE          'RETRAIN'
    END                                                  AS drift_status
FROM psi_calc
