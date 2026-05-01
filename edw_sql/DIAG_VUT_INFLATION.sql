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


-- =============================================================================
-- DIAGNOSIS SUMMARY — what we found, before/after logic, and what NOT to chase
-- =============================================================================
--
-- 1. TOKEN_LIST is a lookup, not a per-client/per-token table.
--    -> 4 columns only: TOKEN_ID, TOKEN_SOURCE, TOKEN_WALLET_IND, TOKEN_NAME.
--    -> Only 13 rows have WALLET_IND='Y'.
--    -> TOKEN_ID identifies the WALLET PROVIDER TYPE (e.g. 40010075001 = GOOGLE PAY).
--       It is shared across every client/card using that wallet. It does NOT
--       identify a unique provisioning event.
--    -> No first-provision date column exists. Provisioning is identified
--       structurally via MIN(TXN_DT) per (card, wallet).
--
-- 2. Why secondary_Nd was inflated ~6x in VVD_SUMMARY_FIXED_WINDOW.sql.
--    -> Wallet tokens, once provisioned, generate recurring zero-dollar
--       keep-alive auths in CLNT_CRD_POS_LOG. Same filter set
--       (AMT1=0, SRVC_CD=36, POS_ENTR_MODE='000', WALLET_IND='Y') matches
--       both the initial provisioning auth AND the recurring auths.
--    -> Old SUM(rows) counted every keep-alive as a separate "provisioning".
--    -> Confirmed in Q2 raw output: client 118107598 had 30+ rows on the
--       SAME (card, wallet) pair, all keep-alives.
--
-- 3. Card-anomaly note (parked, do NOT chase further).
--    -> In tactic 2024232VUT, client 118107598 shows a second VISA_DR_CRD_NO
--       (4519912131874879) with first_auth_dt = 2024-11-09, count = 1.
--    -> This could be any of: card reissue/renewal, client moved to new
--       device, new wallet app, dormant card reactivated. We cannot
--       distinguish these from POS_LOG alone.
--    -> Per the "curated client list" assumption, we treat any first auth
--       inside the campaign window as campaign-attributable. We are NOT
--       trying to filter out "client already had it" cases. Don't go here.
--
-- 4. Before/after logic comparison for VUT and VAW.
--    BEFORE (current VVD_SUMMARY_FIXED_WINDOW.sql, sections "VUT" / "VAW"):
--       primary_Nd   = clients whose MIN(TXN_DT) in [STRT, STRT+89] falls in
--                      [STRT, STRT+N-1].   <-- behaviorally OK, keep it.
--       secondary_Nd = SUM(rows that pass filter, with TXN_DT <= STRT+N-1).
--                      <-- INFLATED by recurring keep-alive auths.
--
--    AFTER (proposed, validated in Q7 above):
--       primary_Nd   = unchanged. distinct clients with first auth in window.
--       secondary_Nd = COUNT distinct (CLNT_CRD_NO, TOKN_REQSTR_ID) pairs
--                      whose first_auth_dt is in [STRT, STRT+N-1].
--                      i.e. one count per unique card-in-wallet provisioning.
--                      A client with 1 card in 2 wallets = 2.
--                      A client with 1 card in 1 wallet (regardless of how
--                      many keep-alive auths) = 1.
--
--    Validation (Q7, tactic 2025160VUT):
--       TG4 90d: distinct_clients=106,966 ; card_wallets=112,363 ; ratio 1.05x
--       TG7 90d: distinct_clients=  5,628 ; card_wallets=  5,893 ; ratio 1.05x
--    1.05x is the realistic small fraction of clients with multiple wallets.
--    Compare to old secondary which gave ~6x.
--
-- 5. Low-hanging fruit (do these).
--    a) Replace secondary_Nd in VVD_SUMMARY_FIXED_WINDOW.sql VUT and VAW
--       sections with the (CLNT_CRD_NO, TOKN_REQSTR_ID) dedup logic from Q7.
--    b) Optionally add TOKEN_NAME to a wallet-breakdown query (Apple Pay
--       vs Google Pay vs other) — same pipeline, just expose the field.
--
-- 6. Rabbit holes to AVOID for now.
--    -> Trying to detect "client was already tokenized pre-campaign" via
--       a wider lookback. Curation list handles this assumption.
--    -> Trying to distinguish initial-provisioning auth vs keep-alive
--       auth from POS_LOG fields alone. No discriminator exists. The
--       (card, wallet, first_auth) collapse is the practical answer.
--    -> Trying to attribute the second VISA_DR_CRD_NO to renewal vs
--       device change vs new wallet. Not knowable from this data.
-- =============================================================================
