-- =============================================================================
-- PAYMENT CAMPAIGN SUMMARIES — FIXED 30 / 60 / 90 day windows
-- Companion to VVD_SUMMARY.sql (which uses TREATMT_END_DT from tactic table)
-- Each query reports success at three milestones from TREATMT_STRT_DT:
--   success_30d  -> day 0 .. day 29 inclusive
--   success_60d  -> day 0 .. day 59 inclusive
--   success_90d  -> day 0 .. day 89 inclusive
-- Columns: mnc | tactic_id | treatmt_strt_dt | test | leads | s_30d | s_60d | s_90d
-- Independent queries, no CTEs, no UNION.
-- =============================================================================


-- VCN — card_acquisition --------------------------------------------------
SELECT
    'VCN'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.ISS_DT <= a.TREATMT_STRT_DT + 29 THEN b.CLNT_NO END)        AS success_30d,
    COUNT(DISTINCT CASE WHEN b.ISS_DT <= a.TREATMT_STRT_DT + 59 THEN b.CLNT_NO END)        AS success_60d,
    COUNT(DISTINCT b.CLNT_NO)                                                              AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DLY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
    AND b.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VDA — card_acquisition --------------------------------------------------
SELECT
    'VDA'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.ISS_DT <= a.TREATMT_STRT_DT + 29 THEN b.CLNT_NO END)        AS success_30d,
    COUNT(DISTINCT CASE WHEN b.ISS_DT <= a.TREATMT_STRT_DT + 59 THEN b.CLNT_NO END)        AS success_60d,
    COUNT(DISTINCT b.CLNT_NO)                                                              AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DLY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
    AND b.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VDT — card_activation ---------------------------------------------------
SELECT
    'VDT'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.ACTV_DT <= a.TREATMT_STRT_DT + 29 THEN b.CLNT_NO END)       AS success_30d,
    COUNT(DISTINCT CASE WHEN b.ACTV_DT <= a.TREATMT_STRT_DT + 59 THEN b.CLNT_NO END)       AS success_60d,
    COUNT(DISTINCT b.CLNT_NO)                                                              AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DLY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.ACTV_DT IS NOT NULL
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
    AND b.ACTV_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VUI — card_usage --------------------------------------------------------
SELECT
    'VUI'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                                                AS leads,
    COUNT(DISTINCT CASE WHEN b.TXN_DT <= a.TREATMT_STRT_DT + 29 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)        AS success_30d,
    COUNT(DISTINCT CASE WHEN b.TXN_DT <= a.TREATMT_STRT_DT + 59 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)        AS success_60d,
    COUNT(DISTINCT SUBSTR(b.CLNT_CRD_NO, 7, 9))                                                              AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 > 0
    AND b.txn_tp IN (10, 13)
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VUT — wallet_provisioning -----------------------------------------------
SELECT
    'VUT'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                                                                            AS leads,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL AND b.TXN_DT <= a.TREATMT_STRT_DT + 29 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_30d,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL AND b.TXN_DT <= a.TREATMT_STRT_DT + 59 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_60d,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL                                       THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
LEFT JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VAW — wallet_provisioning -----------------------------------------------
SELECT
    'VAW'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                                                                            AS leads,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL AND b.TXN_DT <= a.TREATMT_STRT_DT + 29 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_30d,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL AND b.TXN_DT <= a.TREATMT_STRT_DT + 59 THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_60d,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL                                       THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END)         AS success_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
LEFT JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- IPC — imt_success -------------------------------------------------------
SELECT
    'IPC'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.CAPTR_DT <= a.TREATMT_STRT_DT + 29 THEN b.CLNT_NO END)      AS success_30d,
    COUNT(DISTINCT CASE WHEN b.CAPTR_DT <= a.TREATMT_STRT_DT + 59 THEN b.CLNT_NO END)      AS success_60d,
    COUNT(DISTINCT b.CLNT_NO)                                                              AS success_90d
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M a
LEFT JOIN DDWV01.EXT_CDP_CHNL_EVNT b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.ACTVY_TYP_CD = '031'
    AND b.CHNL_TYP_CD IN ('021', '034')
    AND b.SRC_DTA_STORE_CD IN ('139', '140')
    AND b.CAPTR_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'IPC'
  AND a.TACTIC_ID <> '20221891RI'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- IRI — imt_success -------------------------------------------------------
SELECT
    'IRI'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                              AS leads,
    COUNT(DISTINCT CASE WHEN b.CAPTR_DT <= a.TREATMT_STRT_DT + 29 THEN b.CLNT_NO END)      AS success_30d,
    COUNT(DISTINCT CASE WHEN b.CAPTR_DT <= a.TREATMT_STRT_DT + 59 THEN b.CLNT_NO END)      AS success_60d,
    COUNT(DISTINCT b.CLNT_NO)                                                              AS success_90d
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M a
LEFT JOIN DDWV01.EXT_CDP_CHNL_EVNT b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.ACTVY_TYP_CD = '031'
    AND b.CHNL_TYP_CD IN ('021', '034')
    AND b.SRC_DTA_STORE_CD IN ('139', '140')
    AND b.CAPTR_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'IRI'
  AND a.TACTIC_ID <> '20221891RI'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;
