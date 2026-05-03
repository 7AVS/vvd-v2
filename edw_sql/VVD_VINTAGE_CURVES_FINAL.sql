-- =============================================================================
-- VVD VINTAGE CURVES — FINAL
-- Cumulative success curves by vintage day for all 6 VVD campaigns.
-- Pattern mirrors VDT.sql / VDA.sql (the canonical working templates).
--
-- Structure: 6 independent queries, one per campaign. Run each block on its
-- own. VDA / VUT / VAW return primary + secondary in a single output via
-- UNION ALL of the two final SELECTs (METRIC column distinguishes them).
--
-- Output schema (every query):
--   MNE | METRIC | COHORT | TST_GRP_CD | VINTAGE_DAY | LEADS | SUCCESS_CUM
--
-- Window per campaign (matches VVD_SUCCESS_LOGIC_FINAL.sql):
--   VCN  30d   VDA  90d   VDT  30d   VUI  30d   VUT  90d   VAW  30d
--
-- Cost-shape rules (the reason this works under TDWM):
--   1. Pre-filter every EDW success table by `CLNT_NO IN tactic_history`.
--   2. Materialize tactic_history once; reuse for both denominator and joins.
--   3. QUALIFY ROW_NUMBER() = 1 to pick first success per client.
--   4. success_per_day is sparse; final LEFT JOIN sparse → cohort×day grid.
-- =============================================================================


-- VCN — 30d acquisition (PRIMARY only) ----------------------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ISS_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DLY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
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
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
),
success_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM vintage_raw
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)
SELECT
    'VCN'                 AS MNE,
    'PRIMARY_ACQUISITION' AS METRIC,
    g.cohort              AS COHORT,
    g.test                AS TST_GRP_CD,
    g.vintage             AS VINTAGE_DAY,
    g.leads               AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.test
        ORDER BY g.vintage
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN success_per_day spd
    ON g.cohort  = spd.cohort
   AND g.test    = spd.test
   AND g.vintage = spd.vintage
ORDER BY 3, 4, 5
;


-- VDA — 90d acquisition + usage (PRIMARY + SECONDARY) -------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 90
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ISS_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DLY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
),
txn_success AS (
    SELECT
        SUBSTR(c.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        c.TXN_DT                    AS success_dt
    FROM DDWV01.PT_OF_SALE_TXN c
    WHERE c.SRVC_CD = 36
      AND c.MSG_TP = '0220'
      AND c.AMT1 > 0
      AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
          )
      AND c.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-06-29'
      AND SUBSTR(c.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),
primary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN card_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
primary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM primary_vintage WHERE vintage BETWEEN 0 AND 90
    GROUP BY cohort, test, vintage
),
secondary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN txn_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
secondary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM secondary_vintage WHERE vintage BETWEEN 0 AND 90
    GROUP BY cohort, test, vintage
)
SELECT 'VDA' AS MNE, 'PRIMARY_ACQUISITION' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN primary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
UNION ALL
SELECT 'VDA' AS MNE, 'SECONDARY_USAGE' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN secondary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
ORDER BY 2, 3, 4, 5
;


-- VDT — 30d activation (PRIMARY only) -----------------------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
card_success AS (
    SELECT
        b.CLNT_NO,
        b.ACTV_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DLY b
    WHERE b.STS_CD IN ('06', '08')
      AND b.SRVC_ID = 36
      AND b.ACTV_DT IS NOT NULL
      AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
      AND b.CLNT_NO IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
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
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
),
success_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM vintage_raw
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)
SELECT
    'VDT'                AS MNE,
    'PRIMARY_ACTIVATION' AS METRIC,
    g.cohort             AS COHORT,
    g.test               AS TST_GRP_CD,
    g.vintage            AS VINTAGE_DAY,
    g.leads              AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.test
        ORDER BY g.vintage
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN success_per_day spd
    ON g.cohort  = spd.cohort
   AND g.test    = spd.test
   AND g.vintage = spd.vintage
ORDER BY 3, 4, 5
;


-- VUI — 30d usage (PRIMARY only) ----------------------------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
txn_success AS (
    SELECT
        SUBSTR(c.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        c.TXN_DT                    AS success_dt
    FROM DDWV01.PT_OF_SALE_TXN c
    WHERE c.SRVC_CD = 36
      AND c.MSG_TP = '0220'
      AND c.AMT1 > 0
      AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
          )
      AND c.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-04-29'
      AND SUBSTR(c.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),
vintage_raw AS (
    SELECT
        a.cohort,
        a.test,
        (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN txn_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
),
success_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM vintage_raw
    WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)
SELECT
    'VUI'           AS MNE,
    'PRIMARY_USAGE' AS METRIC,
    g.cohort        AS COHORT,
    g.test          AS TST_GRP_CD,
    g.vintage       AS VINTAGE_DAY,
    g.leads         AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.test
        ORDER BY g.vintage
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN success_per_day spd
    ON g.cohort  = spd.cohort
   AND g.test    = spd.test
   AND g.vintage = spd.vintage
ORDER BY 3, 4, 5
;


-- VUT — 90d provisioning + usage (PRIMARY + SECONDARY) ------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 90
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
wallet_success AS (
    SELECT
        SUBSTR(b.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        b.TXN_DT                    AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    INNER JOIN DL_DECMAN.TOKEN_LIST tk
        ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
        AND tk.TOKEN_WALLET_IND = 'Y'
    WHERE b.SRVC_CD = 36
      AND b.AMT1 = 0
      AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND b.APPROVAL_CODE IS NOT NULL
      AND b.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-06-29'
      AND SUBSTR(b.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
txn_success AS (
    SELECT
        SUBSTR(c.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        c.TXN_DT                    AS success_dt
    FROM DDWV01.PT_OF_SALE_TXN c
    WHERE c.SRVC_CD = 36
      AND c.MSG_TP = '0220'
      AND c.AMT1 > 0
      AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
          )
      AND c.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-06-29'
      AND SUBSTR(c.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),
primary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN wallet_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
primary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM primary_vintage WHERE vintage BETWEEN 0 AND 90
    GROUP BY cohort, test, vintage
),
secondary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN txn_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
secondary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM secondary_vintage WHERE vintage BETWEEN 0 AND 90
    GROUP BY cohort, test, vintage
)
SELECT 'VUT' AS MNE, 'PRIMARY_PROVISIONING' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN primary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
UNION ALL
SELECT 'VUT' AS MNE, 'SECONDARY_USAGE' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN secondary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
ORDER BY 2, 3, 4, 5
;


-- VAW — 30d provisioning + usage (PRIMARY + SECONDARY) ------------------------
WITH RECURSIVE day_sequence (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_sequence WHERE day_num < 30
),
tactic_history AS (
    SELECT
        a.CLNT_NO,
        TRIM(TST_GRP_CD)                    AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),
wallet_success AS (
    SELECT
        SUBSTR(b.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        b.TXN_DT                    AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    INNER JOIN DL_DECMAN.TOKEN_LIST tk
        ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
        AND tk.TOKEN_WALLET_IND = 'Y'
    WHERE b.SRVC_CD = 36
      AND b.AMT1 = 0
      AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND b.APPROVAL_CODE IS NOT NULL
      AND b.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-04-29'
      AND SUBSTR(b.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
txn_success AS (
    SELECT
        SUBSTR(c.CLNT_CRD_NO, 7, 9) AS CLNT_NO,
        c.TXN_DT                    AS success_dt
    FROM DDWV01.PT_OF_SALE_TXN c
    WHERE c.SRVC_CD = 36
      AND c.MSG_TP = '0220'
      AND c.AMT1 > 0
      AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
          )
      AND c.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-04-29'
      AND SUBSTR(c.CLNT_CRD_NO, 7, 9) IN (SELECT CLNT_NO FROM tactic_history)
),
denominator AS (
    SELECT cohort, test, COUNT(DISTINCT CLNT_NO) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),
cohort_days AS (
    SELECT d.cohort, d.test, d.leads, s.day_num AS vintage
    FROM denominator d
    CROSS JOIN day_sequence s
),
primary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN wallet_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
primary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM primary_vintage WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
),
secondary_vintage AS (
    SELECT a.cohort, a.test, (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN txn_success b
        ON a.CLNT_NO = b.CLNT_NO
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.CLNT_NO, a.TREATMT_STRT_DT ORDER BY b.success_dt ASC
    ) = 1
),
secondary_per_day AS (
    SELECT cohort, test, vintage, COUNT(*) AS day_success
    FROM secondary_vintage WHERE vintage BETWEEN 0 AND 30
    GROUP BY cohort, test, vintage
)
SELECT 'VAW' AS MNE, 'PRIMARY_PROVISIONING' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN primary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
UNION ALL
SELECT 'VAW' AS MNE, 'SECONDARY_USAGE' AS METRIC,
       g.cohort AS COHORT, g.test AS TST_GRP_CD, g.vintage AS VINTAGE_DAY, g.leads AS LEADS,
       SUM(COALESCE(spd.day_success, 0)) OVER (
           PARTITION BY g.cohort, g.test ORDER BY g.vintage ROWS UNBOUNDED PRECEDING
       ) AS SUCCESS_CUM
FROM cohort_days g
LEFT JOIN secondary_per_day spd
    ON g.cohort = spd.cohort AND g.test = spd.test AND g.vintage = spd.vintage
ORDER BY 2, 3, 4, 5
;
