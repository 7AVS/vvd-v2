-- IPC — IMT Proactive Campaign | Metric: imt_success (first IMT transaction) | Window: 90 days
-- Source tables: DTZV01.TACTIC_EVNT_IP_AR_H60M, DDWV01.EXT_CDP_CHNL_EVNT
-- Output: vintage curve (mnc, cohort, test, vintage, leads, success)
-- Note: day_sequence ensures a complete 0-90 day grid; days with no successes carry forward via cumulative SUM.

WITH tactic_history AS (
    SELECT
        TRIM(CAST(CLNT_NO AS VARCHAR(20)))                AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'IPC'
      AND a.TACTIC_ID <> '20221891RI'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

imt_success AS (
    SELECT
        TRIM(CAST(CLNT_NO AS VARCHAR(20))) AS clnt_no,
        CAPTR_DT                           AS success_dt
    FROM DDWV01.EXT_CDP_CHNL_EVNT
    WHERE ACTVY_TYP_CD = '031'
      AND CHNL_TYP_CD IN ('021', '034')
      AND SRC_DTA_STORE_CD IN ('139', '140')
      AND CAPTR_DT >= DATE '2025-01-01'
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
        SELECT 0 AS n FROM (SELECT 1) x UNION ALL SELECT 1 FROM (SELECT 1) x UNION ALL SELECT 2 FROM (SELECT 1) x UNION ALL SELECT 3 FROM (SELECT 1) x UNION ALL SELECT 4 FROM (SELECT 1) x UNION ALL SELECT 5 FROM (SELECT 1) x UNION ALL SELECT 6 FROM (SELECT 1) x UNION ALL SELECT 7 FROM (SELECT 1) x UNION ALL SELECT 8 FROM (SELECT 1) x UNION ALL SELECT 9 FROM (SELECT 1) x
    ) tens
    CROSS JOIN (
        SELECT 0 AS n FROM (SELECT 1) x UNION ALL SELECT 1 FROM (SELECT 1) x UNION ALL SELECT 2 FROM (SELECT 1) x UNION ALL SELECT 3 FROM (SELECT 1) x UNION ALL SELECT 4 FROM (SELECT 1) x UNION ALL SELECT 5 FROM (SELECT 1) x UNION ALL SELECT 6 FROM (SELECT 1) x UNION ALL SELECT 7 FROM (SELECT 1) x UNION ALL SELECT 8 FROM (SELECT 1) x UNION ALL SELECT 9 FROM (SELECT 1) x
    ) units
    WHERE (tens.n * 10 + units.n) <= 90
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
    JOIN imt_success b
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
    'IPC'  AS mnc,
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
