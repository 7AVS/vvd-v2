-- =============================================================================
-- VVD CAMPAIGN SUMMARIES — 6 independent queries (no CTEs, no UNION)
-- Each query: one row per tactic_id × treatment start date × test group
-- Columns: mnc | tactic_id | treatmt_strt_dt | test (TG4/TG7) | leads | success
-- Window: TREATMT_STRT_DT .. TREATMT_END_DT (per-tactic, set in tactic table)
-- Run each block independently in Teradata.
-- =============================================================================


-- VCN — card_acquisition --------------------------------------------------
SELECT
    'VCN'                                  AS mnc,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT                      AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                     AS test,
    COUNT(DISTINCT a.CLNT_NO)              AS leads,
    COUNT(DISTINCT b.CLNT_NO)              AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DIY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
    AND b.ISS_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
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
    COUNT(DISTINCT a.CLNT_NO)              AS leads,
    COUNT(DISTINCT b.CLNT_NO)              AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DIY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
    AND b.ISS_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
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
    COUNT(DISTINCT a.CLNT_NO)              AS leads,
    COUNT(DISTINCT b.CLNT_NO)              AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV01.VISA_DR_CRD_DIY b
    ON a.CLNT_NO = b.CLNT_NO
    AND b.STS_CD IN ('06', '08')
    AND b.SRVC_ID = 36
    AND b.ACTV_DT IS NOT NULL
    AND b.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
    AND b.ACTV_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VUI — card_usage --------------------------------------------------------
SELECT
    'VUI'                                            AS mnc,
    TRIM(a.TACTIC_ID)                                AS tactic_id,
    a.TREATMT_STRT_DT                                AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                               AS test,
    COUNT(DISTINCT a.CLNT_NO)                        AS leads,
    COUNT(DISTINCT SUBSTR(b.CLNT_CRD_NO, 7, 9))      AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 > 0
    AND b.txn_tp IN (10, 13)
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VUT — wallet_provisioning -----------------------------------------------
SELECT
    'VUT'                                                                                AS mnc,
    TRIM(a.TACTIC_ID)                                                                    AS tactic_id,
    a.TREATMT_STRT_DT                                                                    AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                                                                   AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                            AS leads,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END) AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
LEFT JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;


-- VAW — wallet_provisioning -----------------------------------------------
SELECT
    'VAW'                                                                                AS mnc,
    TRIM(a.TACTIC_ID)                                                                    AS tactic_id,
    a.TREATMT_STRT_DT                                                                    AS treatmt_strt_dt,
    TRIM(a.TST_GRP_CD)                                                                   AS test,
    COUNT(DISTINCT a.CLNT_NO)                                                            AS leads,
    COUNT(DISTINCT CASE WHEN t.TOKEN_ID IS NOT NULL THEN SUBSTR(b.CLNT_CRD_NO, 7, 9) END) AS success
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
LEFT JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
LEFT JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
  AND TRIM(a.TST_GRP_CD) IN ('TG4', 'TG7')
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
;
