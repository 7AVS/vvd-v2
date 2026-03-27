-- IPC — IMT Proactive Campaign | Metric: imt_success (first IMT transaction)
-- Source tables: DTZV01.TACTIC_EVNT_IP_AR_H60M, DDWV01.EXT_CDP_CHNL_EVNT
-- Output: vintage curve (mnc, cohort, test, vintage, leads, success)

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

vintage_raw AS (
    SELECT
        a.test,
        a.cohort,
        (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN imt_success b
        ON a.clnt_no = b.clnt_no
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.clnt_no, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
)

SELECT
    mnc,
    cohort,
    test,
    vintage,
    leads,
    SUM(day_success) OVER (
        PARTITION BY cohort, test
        ORDER BY vintage
        ROWS UNBOUNDED PRECEDING
    ) AS success
FROM (
    SELECT
        'IPC'              AS mnc,
        d.cohort,
        d.test,
        v.vintage,
        d.leads,
        COUNT(*)           AS day_success
    FROM denominator d
    JOIN vintage_raw v
        ON d.cohort = v.cohort
        AND d.test = v.test
    GROUP BY
        d.cohort,
        d.test,
        v.vintage,
        d.leads
) daily
ORDER BY
    cohort,
    test,
    vintage
;
