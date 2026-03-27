-- IRI — IMT Reactive/Trigger Campaign | Metric: imt_success (first IMT transaction) | Window: 30 days
-- Source tables: DTZV01.TACTIC_EVNT_IP_AR_H60M, DDWV01.EXT_CDP_CHNL_EVNT
-- Output: vintage curve (mnc, cohort, test, vintage, leads, success)
-- Note: day_sequence ensures a complete 0-30 day grid; days with no successes carry forward via cumulative SUM.

WITH tactic_history AS (
    SELECT
        TRIM(CAST(CLNT_NO AS VARCHAR(20)))                AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'IRI'
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

day_sequence (day_num) AS (
    SELECT 0
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
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
    'IRI'  AS mnc,
    cd.cohort,
    cd.test,
    cd.vintage,
    cd.leads,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY cd.cohort, cd.test
        ORDER BY cd.vintage
        ROWS UNBOUNDED PRECEDING
    ) AS success
FROM cohort_days cd
LEFT JOIN success_per_day spd
    ON cd.cohort = spd.cohort
    AND cd.test  = spd.test
    AND cd.vintage = spd.vintage
ORDER BY cd.cohort, cd.test, cd.vintage
;
