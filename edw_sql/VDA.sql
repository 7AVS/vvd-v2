-- =============================================================================
-- VDA — Seasonal Acquisition
-- Metric: card_acquisition | Window: 90 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.VISA_DR_CRD_DIY
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-90
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 90
),

-- Step 2: Experiment population — CLNT_NO is a direct column in EDW tactic table
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — card acquisition (new VVD card issued, active status)
--         Join: A.CLNT_NO = B.CLNT_NO (both native columns)
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ISS_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DIY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
),

-- Step 4: Denominator — distinct clients per cohort/test
denominator AS (
    SELECT
        cohort,
        test,
        COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),

-- Step 5: Complete grid — every cohort/test gets days 0-90
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
    JOIN card_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT
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
    WHERE vintage BETWEEN 0 AND 90
    GROUP BY cohort, test, vintage
)

-- Step 8: Final output — cumulative success over the day grid
SELECT
    'VDA'  AS mnc,
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
