-- =============================================================================
-- VVD VINTAGE CURVES — FINAL (slim, no RPT_GRP_CD)
-- Output schema:  MNE | COHORT | TST_GRP_CD | METRIC | VINTAGE_DAY | LEADS | SUCCESS_CUM | RATE
-- Campaigns:      VCN(30d) VDA(90d) VDT(30d) VUI(30d) VUT(90d) VAW(30d)
-- Source filters: verbatim from VVD_SUCCESS_LOGIC_FINAL.sql (locked 2026-05-01)
-- Tactic table:   DG6V01.TACTIC_EVNT_IP_AR_HIST
-- Date range:     TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
-- =============================================================================

WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 89
)

-- =============================================================================
-- VCN PRIMARY — acquisition (card issuance), 30d window
-- =============================================================================
SELECT
    'VCN'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VCN'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 29
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(card.ISS_DT) AS first_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.VISA_DR_CRD_DLY card
            ON a.CLNT_NO = card.CLNT_NO
            AND card.STS_CD IN ('06', '08')
            AND card.SRVC_ID = 36
            AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
            AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VDA PRIMARY — acquisition (card issuance), 90d window
-- =============================================================================
SELECT
    'VDA'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VDA'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 89
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(card.ISS_DT) AS first_dt
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
        INNER JOIN DDWV01.VISA_DR_CRD_DLY card
            ON a.CLNT_NO = card.CLNT_NO
            AND card.STS_CD IN ('06', '08')
            AND card.SRVC_ID = 36
            AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
            AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
        WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
          AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VDA SECONDARY — purchase (PT_OF_SALE_TXN), 90d window
-- =============================================================================
SELECT
    'VDA'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'SECONDARY'                  AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VDA'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 89
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(c.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VDT PRIMARY — activation (ACTV_DT), 30d window
-- =============================================================================
SELECT
    'VDT'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VDT'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 29
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(card.ACTV_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VUI PRIMARY — purchase (PT_OF_SALE_TXN), 30d window
-- =============================================================================
SELECT
    'VUI'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VUI'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 29
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(c.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VUT PRIMARY — wallet provisioning (CLNT_CRD_POS_LOG + TOKEN_LIST), 90d window
-- =============================================================================
SELECT
    'VUT'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VUT'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 89
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(b.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VUT SECONDARY — purchase (PT_OF_SALE_TXN), 90d window
-- =============================================================================
SELECT
    'VUT'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'SECONDARY'                  AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VUT'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 89
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(c.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VAW PRIMARY — wallet provisioning (CLNT_CRD_POS_LOG + TOKEN_LIST), 30d window
-- =============================================================================
SELECT
    'VAW'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'PRIMARY'                    AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VAW'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 29
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(b.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

UNION ALL

-- =============================================================================
-- VAW SECONDARY — purchase (PT_OF_SALE_TXN), 30d window
-- =============================================================================
SELECT
    'VAW'                        AS MNE,
    g.cohort                     AS COHORT,
    g.tst_grp_cd                 AS TST_GRP_CD,
    'SECONDARY'                  AS METRIC,
    g.vintage_day                AS VINTAGE_DAY,
    g.leads                      AS LEADS,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    )                            AS SUCCESS_CUM,
    SUM(COALESCE(s.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM (
    SELECT d.cohort, d.tst_grp_cd, d.leads, ds.day_num AS vintage_day
    FROM (
        SELECT TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
               TRIM(TST_GRP_CD) AS tst_grp_cd,
               COUNT(DISTINCT CLNT_NO) AS leads
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE substr(TACTIC_ID, 8, 3) = 'VAW'
          AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
        GROUP BY 1, 2
    ) d CROSS JOIN day_seq ds WHERE ds.day_num <= 29
) g
LEFT JOIN (
    SELECT TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM') AS cohort,
           TRIM(a.TST_GRP_CD) AS tst_grp_cd,
           fs.first_dt - a.TREATMT_STRT_DT AS vintage_day,
           COUNT(DISTINCT fs.CLNT_NO) AS day_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN (
        SELECT a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, MIN(c.TXN_DT) AS first_dt
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
        GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
    ) fs ON a.CLNT_NO = fs.CLNT_NO AND a.TACTIC_ID = fs.TACTIC_ID AND a.TREATMT_STRT_DT = fs.TREATMT_STRT_DT
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
    GROUP BY 1, 2, 3
) s ON g.cohort = s.cohort AND g.tst_grp_cd = s.tst_grp_cd AND g.vintage_day = s.vintage_day

ORDER BY MNE, COHORT, TST_GRP_CD, METRIC, VINTAGE_DAY
;
