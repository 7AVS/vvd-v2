-- VDT — Activation Trigger | Metric: card_activation | Window: 30 days
-- Source tables: DG6V01.TACTIC_EVNT_IP_AR_HIST, DDWV01.VISA_DR_CRD_DIY
-- Success date: ACTV_DT (activation date, not issuance date)
-- Output: vintage curve (mnc, cohort, test, vintage, leads, success)
-- Note: day_sequence ensures a complete 0-30 day grid; days with no successes carry forward via cumulative SUM.

WITH tactic_history AS (
    SELECT
        TRIM(REGEXP_REPLACE(TACTIC_EVNT_ID, '^0+', ''))  AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

card_success AS (
    SELECT
        TRIM(REGEXP_REPLACE(CAST(CLNT_NO AS VARCHAR(20)), '^0+', '')) AS clnt_no,
        ACTV_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DIY
    WHERE STS_CD IN ('06', '08')
      AND SRVC_ID = 36
      AND ACTV_DT IS NOT NULL
      AND SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
),

denominator AS (
    SELECT
        cohort,
        test,
        COUNT(DISTINCT clnt_no) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),

day_sequence AS (
    SELECT (tens.n * 10 + units.n) AS day_num
    FROM (
        SELECT 0 AS n FROM (SELECT 1) x UNION ALL SELECT 1 FROM (SELECT 1) x UNION ALL SELECT 2 FROM (SELECT 1) x UNION ALL SELECT 3 FROM (SELECT 1) x
    ) tens
    CROSS JOIN (
        SELECT 0 AS n FROM (SELECT 1) x UNION ALL SELECT 1 FROM (SELECT 1) x UNION ALL SELECT 2 FROM (SELECT 1) x UNION ALL SELECT 3 FROM (SELECT 1) x UNION ALL SELECT 4 FROM (SELECT 1) x UNION ALL SELECT 5 FROM (SELECT 1) x UNION ALL SELECT 6 FROM (SELECT 1) x UNION ALL SELECT 7 FROM (SELECT 1) x UNION ALL SELECT 8 FROM (SELECT 1) x UNION ALL SELECT 9 FROM (SELECT 1) x
    ) units
    WHERE (tens.n * 10 + units.n) <= 30
),

cohort_days AS (
    SELECT
        d.cohort,
        d.test,
        d.leads,
        s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),

vintage_raw AS (
    SELECT
        a.cohort,
        a.test,
        (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN card_success b
        ON a.clnt_no = b.clnt_no
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.clnt_no, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
),

success_per_day AS (
    SELECT
        cohort,
        test,
        vintage,
        COUNT(*) AS day_success
    FROM vintage_raw
    GROUP BY cohort, test, vintage
)

SELECT
    'VDT'  AS mnc,
    g.cohort,
    g.test,
    g.vintage,
    g.leads,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.test
        ORDER BY g.vintage
        ROWS UNBOUNDED PRECEDING
    ) AS success
FROM cohort_days g
LEFT JOIN success_per_day spd
    ON g.cohort = spd.cohort
    AND g.test  = spd.test
    AND g.vintage = spd.vintage
ORDER BY g.cohort, g.test, g.vintage
;
