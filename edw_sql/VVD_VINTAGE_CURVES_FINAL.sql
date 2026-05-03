-- =============================================================================
-- VVD VINTAGE CURVES — FINAL
-- Cumulative success curves by vintage day for all 6 VVD campaigns.
--
-- Structure: 6 independent queries, one per campaign. Run each block on its
-- own — each produces its own output table. VDA, VUT, VAW each return BOTH
-- primary and secondary in a single output, distinguished by the METRIC col.
--
-- Output schema (every query):
--   MNE          campaign code (VCN | VDA | VDT | VUI | VUT | VAW)
--   METRIC       PRIMARY_ACQUISITION | PRIMARY_ACTIVATION | PRIMARY_USAGE |
--                PRIMARY_PROVISIONING | SECONDARY_USAGE
--   COHORT       'YYYY-MM' of TREATMT_STRT_DT
--   TST_GRP_CD   '4' (action) or '7' (control), trimmed
--   VINTAGE_DAY  0..29 (30d windows) or 0..89 (90d windows)
--   LEADS        distinct CLNT_NO at cohort × tst_grp_cd (constant across days)
--   SUCCESS_CUM  cumulative distinct successes through VINTAGE_DAY
--
-- Window per campaign (matches VVD_SUCCESS_LOGIC_FINAL.sql):
--   VCN  30d   VDA  90d   VDT  30d   VUI  30d   VUT  90d   VAW  30d
--
-- All filter clauses (success table joins, brand patterns, txn_tp etc.) are
-- copied verbatim from VVD_SUCCESS_LOGIC_FINAL.sql so day-N values reconcile
-- to the FINAL summary metrics.
-- =============================================================================


-- VCN — 30d acquisition (PRIMARY only) ----------------------------------------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VCN'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(s.TST_GRP_CD)                    AS tst_grp_cd,
        (s.first_iss_dt - s.TREATMT_STRT_DT)  AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)             AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(card.ISS_DT) AS first_iss_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.VISA_DR_CRD_DLY card
            ON a.CLNT_NO = card.CLNT_NO
            AND card.STS_CD IN ('06', '08')
            AND card.SRVC_ID = 36
            AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
            AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3
)
SELECT
    'VCN'                 AS MNE,
    'PRIMARY_ACQUISITION' AS METRIC,
    l.cohort              AS COHORT,
    l.tst_grp_cd          AS TST_GRP_CD,
    ds.day_num            AS VINTAGE_DAY,
    l.leads               AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 3, 4, 5
;


-- VDA — 90d acquisition + usage (PRIMARY + SECONDARY in one output) -----------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 89
),
metrics (metric) AS (
    SELECT 'PRIMARY_ACQUISITION' FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT 'SECONDARY_USAGE'     FROM (SELECT 1 AS x) d
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VDA'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        'PRIMARY_ACQUISITION'                  AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(card.ISS_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.VISA_DR_CRD_DLY card
            ON a.CLNT_NO = card.CLNT_NO
            AND card.STS_CD IN ('06', '08')
            AND card.SRVC_ID = 36
            AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
            AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        'SECONDARY_USAGE'                      AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(c.TXN_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.PT_OF_SALE_TXN c
            ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
            AND c.SRVC_CD = 36
            AND c.MSG_TP = '0220'
            AND c.AMT1 > 0
            AND (
                     (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
                  OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
                )
            AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
)
SELECT
    'VDA'        AS MNE,
    m.metric     AS METRIC,
    l.cohort     AS COHORT,
    l.tst_grp_cd AS TST_GRP_CD,
    ds.day_num   AS VINTAGE_DAY,
    l.leads      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY m.metric, l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN metrics m
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.metric      = m.metric
   AND s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 2, 3, 4, 5
;


-- VDT — 30d activation (PRIMARY only) -----------------------------------------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VDT'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(s.TST_GRP_CD)                    AS tst_grp_cd,
        (s.first_actv_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)             AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(card.ACTV_DT) AS first_actv_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.VISA_DR_CRD_DLY card
            ON a.CLNT_NO = card.CLNT_NO
            AND card.STS_CD IN ('06', '08')
            AND card.SRVC_ID = 36
            AND card.ACTV_DT IS NOT NULL
            AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
            AND card.ACTV_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3
)
SELECT
    'VDT'                AS MNE,
    'PRIMARY_ACTIVATION' AS METRIC,
    l.cohort             AS COHORT,
    l.tst_grp_cd         AS TST_GRP_CD,
    ds.day_num           AS VINTAGE_DAY,
    l.leads              AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 3, 4, 5
;


-- VUI — 30d usage (PRIMARY only) ----------------------------------------------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VUI'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(s.TST_GRP_CD)                    AS tst_grp_cd,
        (s.first_txn_dt - s.TREATMT_STRT_DT)  AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)             AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(c.TXN_DT) AS first_txn_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.PT_OF_SALE_TXN c
            ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
            AND c.SRVC_CD = 36
            AND c.MSG_TP = '0220'
            AND c.AMT1 > 0
            AND (
                     (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
                  OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
                )
            AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3
)
SELECT
    'VUI'           AS MNE,
    'PRIMARY_USAGE' AS METRIC,
    l.cohort        AS COHORT,
    l.tst_grp_cd    AS TST_GRP_CD,
    ds.day_num      AS VINTAGE_DAY,
    l.leads         AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 3, 4, 5
;


-- VUT — 90d provisioning + usage (PRIMARY + SECONDARY in one output) ----------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 89
),
metrics (metric) AS (
    SELECT 'PRIMARY_PROVISIONING' FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT 'SECONDARY_USAGE'      FROM (SELECT 1 AS x) d
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VUT'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        'PRIMARY_PROVISIONING'                 AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(b.TXN_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
            ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
            AND b.SRVC_CD = 36
            AND b.AMT1 = 0
            AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
            AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
            AND b.APPROVAL_CODE IS NOT NULL
            AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
        INNER JOIN DL_DECMAN.TOKEN_LIST tk
            ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
            AND tk.TOKEN_WALLET_IND = 'Y'
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        'SECONDARY_USAGE'                      AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(c.TXN_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.PT_OF_SALE_TXN c
            ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
            AND c.SRVC_CD = 36
            AND c.MSG_TP = '0220'
            AND c.AMT1 > 0
            AND (
                     (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
                  OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
                )
            AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
)
SELECT
    'VUT'        AS MNE,
    m.metric     AS METRIC,
    l.cohort     AS COHORT,
    l.tst_grp_cd AS TST_GRP_CD,
    ds.day_num   AS VINTAGE_DAY,
    l.leads      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY m.metric, l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN metrics m
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.metric      = m.metric
   AND s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 2, 3, 4, 5
;


-- VAW — 30d provisioning + usage (PRIMARY + SECONDARY in one output) ----------
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 AS day_num FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),
metrics (metric) AS (
    SELECT 'PRIMARY_PROVISIONING' FROM (SELECT 1 AS x) d
    UNION ALL
    SELECT 'SECONDARY_USAGE'      FROM (SELECT 1 AS x) d
),
leads AS (
    SELECT
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
        TRIM(TST_GRP_CD)                    AS tst_grp_cd,
        COUNT(DISTINCT CLNT_NO)             AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VAW'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
),
daily_success AS (
    SELECT
        'PRIMARY_PROVISIONING'                 AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(b.TXN_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
            ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
            AND b.SRVC_CD = 36
            AND b.AMT1 = 0
            AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
            AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
            AND b.APPROVAL_CODE IS NOT NULL
            AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        INNER JOIN DL_DECMAN.TOKEN_LIST tk
            ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
            AND tk.TOKEN_WALLET_IND = 'Y'
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT
        'SECONDARY_USAGE'                      AS metric,
        TO_CHAR(s.TREATMT_STRT_DT, 'YYYY-MM')  AS cohort,
        TRIM(s.TST_GRP_CD)                     AS tst_grp_cd,
        (s.first_event_dt - s.TREATMT_STRT_DT) AS vintage_day,
        COUNT(DISTINCT s.CLNT_NO)              AS day_success
    FROM (
        SELECT
            a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT,
            MIN(c.TXN_DT) AS first_event_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.PT_OF_SALE_TXN c
            ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
            AND c.SRVC_CD = 36
            AND c.MSG_TP = '0220'
            AND c.AMT1 > 0
            AND (
                     (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
                  OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
                )
            AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TST_GRP_CD, a.TREATMT_STRT_DT
    ) s
    GROUP BY 1, 2, 3, 4
)
SELECT
    'VAW'        AS MNE,
    m.metric     AS METRIC,
    l.cohort     AS COHORT,
    l.tst_grp_cd AS TST_GRP_CD,
    ds.day_num   AS VINTAGE_DAY,
    l.leads      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY m.metric, l.cohort, l.tst_grp_cd
        ORDER BY ds.day_num
        ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM
FROM leads l
CROSS JOIN metrics m
CROSS JOIN day_seq ds
LEFT JOIN daily_success s
    ON s.metric      = m.metric
   AND s.cohort      = l.cohort
   AND s.tst_grp_cd  = l.tst_grp_cd
   AND s.vintage_day = ds.day_num
ORDER BY 2, 3, 4, 5
;
