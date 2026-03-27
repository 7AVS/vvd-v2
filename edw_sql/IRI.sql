-- =============================================================================
-- IRI — IMT Reactive/Trigger
-- Metric: imt_success (first IMT transaction) | Window: 30 days
-- Source: DTZV01.TACTIC_EVNT_IP_AR_H60M + DDWV01.EXT_CDP_CHNL_EVNT
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-30
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),

-- Step 2: Experiment population — all IRI clients (Action TG4, Control TG7)
tactic_history AS (
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

-- Step 3: Success — IMT transaction (mobile or online banking)
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

-- Step 4: Denominator — distinct clients per cohort/test
denominator AS (
    SELECT
        cohort,
        test,
        COUNT(DISTINCT clnt_no) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),

-- Step 5: Complete grid — every cohort/test gets days 0-30
cohort_days AS (
    SELECT
        d.cohort,
        d.test,
        d.leads,
        s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),

-- Step 6: First success per client — days from treatment start
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

-- Step 7: Count successes per day
success_per_day AS (
    SELECT
        cohort,
        test,
        vintage,
        COUNT(*) AS day_success
    FROM vintage_raw
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)

-- Step 8: Final output — cumulative success over the day grid
SELECT
    'IRI'  AS mnc,
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
