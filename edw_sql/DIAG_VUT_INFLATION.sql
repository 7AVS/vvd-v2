-- =============================================================================
-- VUT — diagnostic: identify and inspect row-count inflation in secondary_Nd
-- Step 1: find clients with high row counts and breakdown by distinct
--         cards / token_requestors / token_ids / dates.
-- Step 2: paste a CLNT_NO from Step 1 into Query 2 to see the raw rows
--         and identify which field is multiplying.
-- =============================================================================


-- Query 1 — top inflated clients, with diagnostic counts ------------------
SELECT
    a.CLNT_NO,
    TRIM(a.TACTIC_ID)                      AS tactic_id,
    a.TREATMT_STRT_DT,
    COUNT(*)                               AS row_count,
    COUNT(DISTINCT b.CLNT_CRD_NO)          AS distinct_cards,
    COUNT(DISTINCT b.TOKN_REQSTR_ID)       AS distinct_tokn_reqstrs,
    COUNT(DISTINCT t.TOKEN_ID)             AS distinct_token_ids,
    COUNT(DISTINCT b.TXN_DT)               AS distinct_txn_dates
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
INNER JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
HAVING COUNT(*) >= 5
ORDER BY row_count DESC
;

-- Interpretation:
-- row_count = 6, distinct_cards = 6                            -> real 6 cards (rare)
-- row_count = 6, distinct_cards = 1, distinct_token_ids = 6    -> token_list multiplication
-- row_count = 6, distinct_cards = 1, distinct_token_ids = 1    -> POS row duplicates
-- row_count = 6, distinct_cards = 1, distinct_tokn_reqstrs = 6 -> multiple provisioning attempts


-- Query 2 — raw rows for one client (paste a CLNT_NO from Query 1) --------
SELECT
    a.CLNT_NO,
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    b.CLNT_CRD_NO,
    b.VISA_DR_CRD_NO,
    b.TXN_DT,
    b.AMT1,
    b.SRVC_CD,
    b.POS_ENTR_MODE_CD_NON_EMV,
    b.APPROVAL_CODE,
    b.TOKN_REQSTR_ID,
    t.TOKEN_ID,
    t.TOKEN_WALLET_IND
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
INNER JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE a.CLNT_NO = <PASTE_A_CLNT_NO_HERE>
  AND substr(a.TACTIC_ID, 8, 3) = 'VUT'
ORDER BY b.TXN_DT, b.CLNT_CRD_NO
;


-- =============================================================================
-- Exploratory — TOKEN_LIST schema and sample, looking for first-provision flag
-- Goal: see whether TOKEN_LIST has a column that distinguishes the initial
-- provisioning event from the recurring zero-dollar keep-alive auths that
-- show up in CLNT_CRD_POS_LOG.
-- =============================================================================


-- Query 3 — TOKEN_LIST column list -----------------------------------------
SELECT ColumnName, ColumnType, ColumnLength, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DL_DECMAN'
  AND TableName    = 'TOKEN_LIST'
ORDER BY ColumnId
;


-- Query 4 — sample rows for the token observed in CLNT_NO 118107598 -------
SELECT *
FROM DL_DECMAN.TOKEN_LIST
WHERE TOKEN_ID = 40010075001
QUALIFY ROW_NUMBER() OVER (ORDER BY TOKEN_ID) <= 20
;


-- Query 5 — distribution of TOKEN_WALLET_IND ------------------------------
SELECT TOKEN_WALLET_IND, COUNT(*) AS row_count
FROM DL_DECMAN.TOKEN_LIST
GROUP BY 1
ORDER BY 2 DESC
;


-- =============================================================================
-- Recommended fix — dedup at (CLNT_CRD_NO, TOKN_REQSTR_ID) per (tactic, client)
-- One row per card-wallet pair, with earliest qualifying auth in campaign
-- window and the count of recurring auths. Run with the same CLNT_NO used
-- in Query 2 to see the difference.
-- =============================================================================


-- Query 6 — per card-wallet pair: first auth + count -----------------------
SELECT
    a.CLNT_NO,
    TRIM(a.TACTIC_ID)                               AS tactic_id,
    a.TREATMT_STRT_DT,
    b.CLNT_CRD_NO,
    b.VISA_DR_CRD_NO,
    b.TOKN_REQSTR_ID,
    t.TOKEN_ID,
    MIN(b.TXN_DT)                                   AS first_auth_dt,
    COUNT(*)                                        AS auth_count_in_window
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.AMT1 = 0
    AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
    AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
    AND b.APPROVAL_CODE IS NOT NULL
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
INNER JOIN DL_DECMAN.TOKEN_LIST t
    ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    AND t.TOKEN_WALLET_IND = 'Y'
WHERE a.CLNT_NO = <PASTE_A_CLNT_NO_HERE>
  AND substr(a.TACTIC_ID, 8, 3) = 'VUT'
GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT,
         b.CLNT_CRD_NO, b.VISA_DR_CRD_NO, b.TOKN_REQSTR_ID, t.TOKEN_ID
ORDER BY b.CLNT_CRD_NO, b.TOKN_REQSTR_ID
;


-- Query 7 — same logic at tactic level (preview of new primary/secondary) -
-- Shows what aggregated counts WOULD look like with card-wallet dedup,
-- so you can compare against the inflated secondary_30d in
-- VVD_SUMMARY_FIXED_WINDOW.sql.
SELECT
    TRIM(t_in.tactic_id)                                                                       AS tactic_id,
    t_in.treatmt_strt_dt,
    TRIM(t_in.test)                                                                            AS test,
    COUNT(DISTINCT t_in.CLNT_NO)                                                               AS distinct_clients,
    SUM(CASE WHEN t_in.first_auth_dt <= t_in.treatmt_strt_dt + 29 THEN 1 ELSE 0 END)           AS card_wallets_30d,
    SUM(CASE WHEN t_in.first_auth_dt <= t_in.treatmt_strt_dt + 59 THEN 1 ELSE 0 END)           AS card_wallets_60d,
    SUM(CASE WHEN t_in.first_auth_dt IS NOT NULL                  THEN 1 ELSE 0 END)           AS card_wallets_90d
FROM (
    SELECT
        a.CLNT_NO,
        TRIM(a.TACTIC_ID)                                       AS tactic_id,
        a.TREATMT_STRT_DT                                       AS treatmt_strt_dt,
        TRIM(a.TST_GRP_CD)                                      AS test,
        b.CLNT_CRD_NO,
        b.TOKN_REQSTR_ID,
        MIN(b.TXN_DT)                                           AS first_auth_dt
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT, a.TST_GRP_CD,
             b.CLNT_CRD_NO, b.TOKN_REQSTR_ID
) t_in
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
