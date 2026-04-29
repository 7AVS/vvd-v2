-- =============================================================================
-- VVD ALL CAMPAIGNS — bundled from edw_sql/{VCN,VDA,VDT,VUI,VUT,VAW}.sql
-- Each section is the verbatim contents of the per-campaign file.
-- Run sections individually in Teradata; this file is for reference,
-- not single-shot execution (each section ends with a semicolon).
--
-- Campaign | Metric              | Window | Source tables
-- ---------|---------------------|--------|--------------------------------------
-- VCN      | card_acquisition    |  30d   | TACTIC_EVNT_IP_AR_HIST + VISA_DR_CRD_DIY
-- VDA      | card_acquisition    |  90d   | TACTIC_EVNT_IP_AR_HIST + VISA_DR_CRD_DIY
-- VDT      | card_activation     |  30d   | TACTIC_EVNT_IP_AR_HIST + VISA_DR_CRD_DIY
-- VUI      | card_usage          |  30d   | TACTIC_EVNT_IP_AR_HIST + CLNT_CRD_POS_LOG
-- VUT      | wallet_provisioning |  90d   | TACTIC_EVNT_IP_AR_HIST + CLNT_CRD_POS_LOG + TOKEN_LIST
-- VAW      | wallet_provisioning |  30d   | TACTIC_EVNT_IP_AR_HIST + CLNT_CRD_POS_LOG + TOKEN_LIST
-- =============================================================================


-- #############################################################################
-- ## SECTION 1 of 6 — VCN
-- ## Source: edw_sql/VCN.sql
-- #############################################################################

-- =============================================================================
-- VCN — Contextual Notification
-- Metric: card_acquisition | Window: 30 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.VISA_DR_CRD_DIY
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-30
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
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
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — card acquisition (new VVD card issued, active status)
--         CLNT_NO is a direct column on VISA_DR_CRD_DIY
--         Join: A.CLNT_NO = B.CLNT_NO (both native columns)
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ISS_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DIY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
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
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)

-- Step 8: Final output — cumulative success over the day grid
SELECT
    'VCN'  AS mnc,
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


-- #############################################################################
-- ## SECTION 2 of 6 — VDA
-- ## Source: edw_sql/VDA.sql
-- #############################################################################

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
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
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


-- #############################################################################
-- ## SECTION 3 of 6 — VDT
-- ## Source: edw_sql/VDT.sql
-- #############################################################################

-- =============================================================================
-- VDT — Activation Trigger
-- Metric: card_activation | Window: 30 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.VISA_DR_CRD_DIY
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-30
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
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
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — card activation (ACTV_DT, not ISS_DT)
--         Join: A.CLNT_NO = B.CLNT_NO (both native columns)
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ACTV_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DIY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.ACTV_DT IS NOT NULL
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
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
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)

-- Step 8: Final output — cumulative success over the day grid
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


-- #############################################################################
-- ## SECTION 4 of 6 — VUI
-- ## Source: edw_sql/VUI.sql
-- #############################################################################

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

-- Step 2: Experiment population — CLNT_NO is a direct column in EDW tactic table
tactic_history AS (
    SELECT
        a.CLNT_NO,
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
--         CLNT_NO derived from CLNT_CRD_NO positions 7-9 (card-level → client-level)
--         Join: A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO, 7, 9) per SAS source of truth
txn_success AS (
    SELECT
        SUBSTR(b.CLNT_CRD_NO, 7, 9) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    WHERE b.SRVC_CD = 36
      AND b.AMT1 > 0
      AND b.txn_tp IN (10, 13)
      AND b.TXN_DT >= DATE '2025-01-01'
      AND SUBSTR(b.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
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
        ON a.CLNT_NO = b.clnt_no
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


-- #############################################################################
-- ## SECTION 5 of 6 — VUT
-- ## Source: edw_sql/VUT.sql
-- #############################################################################

-- =============================================================================
-- VUT — Tokenization
-- Metric: wallet_provisioning | Window: 90 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV05.CLNT_CRD_POS_LOG + DL_DECMAN.TOKEN_LIST
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
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — wallet provisioning (zero-dollar token txn, VVD BIN, confirmed wallet)
--         CLNT_NO derived from CLNT_CRD_NO positions 7-9 (card-level → client-level)
--         Join: A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO, 7, 9) per SAS source of truth
wallet_success AS (
    SELECT
        SUBSTR(b.CLNT_CRD_NO, 7, 9) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    JOIN DL_DECMAN.TOKEN_LIST t
        ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    WHERE b.SRVC_CD = 36
      AND b.AMT1 = 0
      AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND b.APPROVAL_CODE IS NOT NULL
      AND t.TOKEN_WALLET_IND = 'Y'
      AND b.TXN_DT >= DATE '2025-01-01'
      AND SUBSTR(b.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
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
    JOIN wallet_success b
        ON a.CLNT_NO = b.clnt_no
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
    'VUT'  AS mnc,
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


-- #############################################################################
-- ## SECTION 6 of 6 — VAW
-- ## Source: edw_sql/VAW.sql
-- #############################################################################

-- =============================================================================
-- VAW — Add To Wallet
-- Metric: wallet_provisioning | Window: 30 days
-- Source: DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV05.CLNT_CRD_POS_LOG + DL_DECMAN.TOKEN_LIST
-- Output: mnc, cohort, test, vintage, leads, success (cumulative)
-- =============================================================================

-- Step 1: Generate day grid 0-30
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
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
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

-- Step 3: Success — wallet provisioning (zero-dollar token txn, VVD BIN, confirmed wallet)
--         CLNT_NO derived from CLNT_CRD_NO positions 7-9 (card-level → client-level)
--         Join uses SUBSTR directly per SAS source of truth: A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO, 7, 9)
wallet_success AS (
    SELECT
        SUBSTR(b.CLNT_CRD_NO, 7, 9) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    JOIN DL_DECMAN.TOKEN_LIST t
        ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    WHERE b.SRVC_CD = 36
      AND b.AMT1 = 0
      AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND b.APPROVAL_CODE IS NOT NULL
      AND t.TOKEN_WALLET_IND = 'Y'
      AND b.TXN_DT >= DATE '2025-01-01'
      AND SUBSTR(b.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
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
    JOIN wallet_success b
        ON a.CLNT_NO = b.clnt_no
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
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)

-- Step 8: Final output — cumulative success over the day grid
SELECT
    'VAW'  AS mnc,
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
