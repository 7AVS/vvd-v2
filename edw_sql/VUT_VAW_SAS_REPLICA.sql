-- =============================================================================
-- VUT / VAW — Teradata replica of the SAS Success1a -> Success1b
--             -> tactic_flags2d pipeline
--
-- Pipeline (compressed into one query per campaign):
--   1. Inner subquery = SAS Success1a:
--        INNER JOIN tactic + POS_LOG + TOKEN_LIST, group by (client, tactic),
--        MIN(TXN_DT) over the window  STRT  ..  END_DT + 30
--   2. Outer LEFT JOIN merges back to full tactic population (= work.tactic),
--      COALESCE missing min_dt to NULL,
--      SUCCESS1_IND = 1 when min_dt BETWEEN STRT AND STRT+30, else 0.
--   3. Aggregate to tactic x test_group level for direct comparison
--      with the SAS proc freq output.
--
-- Filters mirrored verbatim from SAS:
--   AMT1 = 0
--   SUBSTR(CLNT_CRD_NO,    1, 5) = '45190'
--   SUBSTR(VISA_DR_CRD_NO, 1, 5) = '45199'
--   SUBSTR(TOKN_REQSTR_ID, 1, 1) > '0'
--   POS_ENTR_MODE_CD_NON_EMV = '000'
--   TOKEN_WALLET_IND = 'Y'
--   SRVC_CD = 36
--
-- Output: tactic_id | treatmt_strt_dt | test | leads | success_30d
-- =============================================================================


-- VUT --------------------------------------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                     AS tactic_id,
    t.TREATMT_STRT_DT                                                                     AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                    AS test,
    COUNT(*)                                                                              AS leads,
    SUM(CASE WHEN s.min_dt BETWEEN t.TREATMT_STRT_DT AND (t.TREATMT_STRT_DT + 30)
             THEN 1 ELSE 0 END)                                                           AS success_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT) AS min_dt
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
) s
    ON  t.CLNT_NO         = s.CLNT_NO
    AND t.TACTIC_ID       = s.TACTIC_ID
    AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VUT'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- VAW --------------------------------------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                     AS tactic_id,
    t.TREATMT_STRT_DT                                                                     AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                    AS test,
    COUNT(*)                                                                              AS leads,
    SUM(CASE WHEN s.min_dt BETWEEN t.TREATMT_STRT_DT AND (t.TREATMT_STRT_DT + 30)
             THEN 1 ELSE 0 END)                                                           AS success_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT) AS min_dt
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
) s
    ON  t.CLNT_NO         = s.CLNT_NO
    AND t.TACTIC_ID       = s.TACTIC_ID
    AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VAW'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
