-- =============================================================================
-- VUT / VAW — Teradata replica of the SAS Success1a -> Success1b
--             -> tactic_flags2d pipeline, with primary AND secondary success.
--
-- Inner subquery, one row per (tactic, client), computes:
--   min_dt     = earliest qualifying provisioning over [STRT, END_DT+30]
--                (SAS-style wide net for first-event semantics)
--   events_30d = count of qualifying provisionings strictly in [STRT, STRT+30]
--                (a client with multiple cards contributes multiple)
--
-- Outer query rolls up to tactic_id x treatmt_strt_dt x test:
--   leads         = clients in tactic
--   primary_30d   = clients whose FIRST provisioning falls in [STRT, STRT+30]
--                   (1 per client max -- SAS Success1_ind logic)
--   secondary_30d = total provisioning events in [STRT, STRT+30]
--                   (multi-card client counted multiple times)
--
-- Filters mirrored verbatim from SAS:
--   AMT1 = 0
--   SUBSTR(CLNT_CRD_NO,    1, 5) = '45190'
--   SUBSTR(VISA_DR_CRD_NO, 1, 5) = '45199'
--   SUBSTR(TOKN_REQSTR_ID, 1, 1) > '0'
--   POS_ENTR_MODE_CD_NON_EMV = '000'
--   TOKEN_WALLET_IND = 'Y'
--   SRVC_CD = 36
-- =============================================================================


-- VUT --------------------------------------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                                           AS tactic_id,
    t.TREATMT_STRT_DT                                                                                           AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                          AS test,
    COUNT(*)                                                                                                    AS leads,
    SUM(CASE WHEN x.min_dt BETWEEN t.TREATMT_STRT_DT AND (t.TREATMT_STRT_DT + 30) THEN 1 ELSE 0 END)            AS primary_30d,
    SUM(COALESCE(x.events_30d, 0))                                                                              AS secondary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT)                                                                                          AS min_dt,
        SUM(CASE WHEN b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 30) THEN 1 ELSE 0 END)        AS events_30d
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
        ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    INNER JOIN DL_DECMAN.TOKEN_LIST c
        ON b.TOKN_REQSTR_ID = c.TOKEN_ID
    WHERE b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_END_DT + 30)
      AND b.AMT1 = 0
      AND SUBSTR(b.CLNT_CRD_NO,    1, 5) = '45190'
      AND SUBSTR(b.VISA_DR_CRD_NO, 1, 5) = '45199'
      AND SUBSTR(b.TOKN_REQSTR_ID, 1, 1) > '0'
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND c.TOKEN_WALLET_IND = 'Y'
      AND b.SRVC_CD = 36
      AND substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) x
    ON  t.CLNT_NO         = x.CLNT_NO
    AND t.TACTIC_ID       = x.TACTIC_ID
    AND t.TREATMT_STRT_DT = x.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VUT'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- VAW --------------------------------------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                                           AS tactic_id,
    t.TREATMT_STRT_DT                                                                                           AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                          AS test,
    COUNT(*)                                                                                                    AS leads,
    SUM(CASE WHEN x.min_dt BETWEEN t.TREATMT_STRT_DT AND (t.TREATMT_STRT_DT + 30) THEN 1 ELSE 0 END)            AS primary_30d,
    SUM(COALESCE(x.events_30d, 0))                                                                              AS secondary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT)                                                                                          AS min_dt,
        SUM(CASE WHEN b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 30) THEN 1 ELSE 0 END)        AS events_30d
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
        ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    INNER JOIN DL_DECMAN.TOKEN_LIST c
        ON b.TOKN_REQSTR_ID = c.TOKEN_ID
    WHERE b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_END_DT + 30)
      AND b.AMT1 = 0
      AND SUBSTR(b.CLNT_CRD_NO,    1, 5) = '45190'
      AND SUBSTR(b.VISA_DR_CRD_NO, 1, 5) = '45199'
      AND SUBSTR(b.TOKN_REQSTR_ID, 1, 1) > '0'
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND c.TOKEN_WALLET_IND = 'Y'
      AND b.SRVC_CD = 36
      AND substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) x
    ON  t.CLNT_NO         = x.CLNT_NO
    AND t.TACTIC_ID       = x.TACTIC_ID
    AND t.TREATMT_STRT_DT = x.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VAW'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
