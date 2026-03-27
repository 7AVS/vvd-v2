-- =============================================================================
-- VUI — Usage Trigger
-- Metric: card_usage (purchase transaction) | Window: 30 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV05.CLNT_CRD_POS_LOG
-- TODO: Confirm POS table name — may be DDWV01.PT_OF_SALE_TXN
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-30
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),

-- Step 2: Experiment population — all VUI clients (Action TG4, Control TG7)
tactic_history AS (
    SELECT
        TRIM(REGEXP_REPLACE(TACTIC_EVNT_ID, '^0+', ''))  AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — card usage (purchase txn, non-zero amount, VVD service code)
txn_success AS (
    SELECT
        TRIM(LEADING '0' FROM SUBSTR(b.CLNT_CRD_NO, 7, 9)) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    WHERE b.SRVC_CD = 36
      AND b.AMT1 > 0
      AND b.txn_tp IN (10, 13)
      AND b.TXN_DT >= DATE '2025-01-01'
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
    JOIN txn_success b
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
    'VUI'  AS mnc,
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
