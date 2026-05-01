-- =============================================================================
-- VUI — diagnostic: compare PT_OF_SALE_TXN (SAS) vs CLNT_CRD_POS_LOG (our code)
-- and the AMT1>0 / TXN_TP filter combinations.
--
-- Goal: figure out whether
--   (a) our table choice (CLNT_CRD_POS_LOG) is correct
--   (b) our additional filters (AMT1>0, TXN_TP IN (10,13)) are correct
-- by counting successful clients three ways for the same VUI tactic set.
-- =============================================================================


-- Query 1 — schema for PT_OF_SALE_TXN -------------------------------------
SELECT 'PT_OF_SALE_TXN' AS table_name, ColumnName, ColumnType, ColumnLength, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DDWV01'
  AND TableName    = 'PT_OF_SALE_TXN'
ORDER BY ColumnId
;


-- Query 2 — schema for CLNT_CRD_POS_LOG -----------------------------------
SELECT 'CLNT_CRD_POS_LOG' AS table_name, ColumnName, ColumnType, ColumnLength, Nullable
FROM DBC.COLUMNS
WHERE DatabaseName = 'DDWV05'
  AND TableName    = 'CLNT_CRD_POS_LOG'
ORDER BY ColumnId
;


-- Query 3 — side-by-side success count under 3 definitions ---------------
-- For each VUI tactic, count distinct successful clients (any qualifying
-- TXN_DT in [STRT, STRT+89]) under three approaches:
--   sas_success      = PT_OF_SALE_TXN, no AMT/TXN_TP filter (SAS as-is)
--   hybrid_success   = PT_OF_SALE_TXN, with AMT1>0 + TXN_TP IN (10,13)
--   ours_success     = CLNT_CRD_POS_LOG, with AMT1>0 + TXN_TP IN (10,13)
SELECT
    pop.tactic_id,
    pop.treatmt_strt_dt,
    pop.leads,
    sas.sas_success,
    hyb.hybrid_success,
    ours.ours_success
FROM (
    SELECT
        TRIM(TACTIC_ID)        AS tactic_id,
        TREATMT_STRT_DT        AS treatmt_strt_dt,
        COUNT(DISTINCT CLNT_NO) AS leads
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE substr(TACTIC_ID, 8, 3) = 'VUI'
      AND TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
) pop

LEFT JOIN (
    SELECT
        TRIM(a.TACTIC_ID)         AS tactic_id,
        a.TREATMT_STRT_DT         AS treatmt_strt_dt,
        COUNT(DISTINCT a.CLNT_NO) AS sas_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
) sas
    ON pop.tactic_id = sas.tactic_id
    AND pop.treatmt_strt_dt = sas.treatmt_strt_dt

LEFT JOIN (
    SELECT
        TRIM(a.TACTIC_ID)         AS tactic_id,
        a.TREATMT_STRT_DT         AS treatmt_strt_dt,
        COUNT(DISTINCT a.CLNT_NO) AS hybrid_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.AMT1 > 0
        AND c.TXN_TP IN ('10', '13')
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
) hyb
    ON pop.tactic_id = hyb.tactic_id
    AND pop.treatmt_strt_dt = hyb.treatmt_strt_dt

LEFT JOIN (
    SELECT
        TRIM(a.TACTIC_ID)         AS tactic_id,
        a.TREATMT_STRT_DT         AS treatmt_strt_dt,
        COUNT(DISTINCT a.CLNT_NO) AS ours_success
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
        ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
        AND b.SRVC_CD = 36
        AND b.AMT1 > 0
        AND b.txn_tp IN (10, 13)
        AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY 1, 2
) ours
    ON pop.tactic_id = ours.tactic_id
    AND pop.treatmt_strt_dt = ours.treatmt_strt_dt

ORDER BY pop.tactic_id, pop.treatmt_strt_dt
;

-- Interpretation:
--   sas_success == hybrid_success  -> AMT/TXN_TP filters don't matter on
--                                     PT_OF_SALE_TXN (table is already
--                                     pre-filtered). Tables differ between
--                                     SAS and our approach only.
--   sas_success != hybrid_success  -> filters matter on PT_OF_SALE_TXN too.
--   hybrid_success == ours_success -> tables are equivalent under the same
--                                     filter set. Filters drive everything.
--   hybrid_success != ours_success -> tables are genuinely different. Need
--                                     to pick which is "right" for VUI.


-- Query 4 — pick a sample client from a tactic where SAS != ours ---------
-- Run after Query 3, paste a (tactic_id, CLNT_NO) of interest into Q5/Q6.
-- Heuristic: pick a tactic where sas_success - ours_success is largest.


-- Query 5 — raw rows from PT_OF_SALE_TXN for one client ------------------
SELECT
    a.CLNT_NO,
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    c.CLNT_CRD_NO,
    c.TXN_DT,
    c.AMT1,
    c.SRVC_CD,
    c.TXN_TP
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV01.PT_OF_SALE_TXN c
    ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
    AND c.SRVC_CD = 36
    AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE a.CLNT_NO = <PASTE_A_CLNT_NO_HERE>
  AND substr(a.TACTIC_ID, 8, 3) = 'VUI'
ORDER BY c.TXN_DT
;


-- Query 6 — raw rows from CLNT_CRD_POS_LOG for the SAME client -----------
SELECT
    a.CLNT_NO,
    a.TACTIC_ID,
    a.TREATMT_STRT_DT,
    b.CLNT_CRD_NO,
    b.TXN_DT,
    b.AMT1,
    b.SRVC_CD,
    b.txn_tp
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE a.CLNT_NO = <PASTE_A_CLNT_NO_HERE>
  AND substr(a.TACTIC_ID, 8, 3) = 'VUI'
ORDER BY b.TXN_DT
;

-- Q5 vs Q6 inspection:
--   Same dates and TXN_TP / AMT1 values -> tables are duplicates of each
--   other under SRVC_CD=36 + window filter. Either is fine.
--   Different rows -> investigate. Note in particular whether
--   CLNT_CRD_POS_LOG carries auth pings / non-purchase txn types that
--   PT_OF_SALE_TXN strips out.


-- =============================================================================
-- TXN_TP / MSG_TP exploration — is the (10,13) purchase filter too narrow?
-- Goal: see every distinct (TXN_TP, MSG_TP) combination present in
-- PT_OF_SALE_TXN for VUI tactic clients in the campaign window. Identify
-- which codes look like real card usage (subscriptions, recurring, CNP,
-- online) vs noise (auths, reversals, voids, declines).
-- =============================================================================


-- Query 7 — TXN_TP / MSG_TP distribution in PT_OF_SALE_TXN ----------------
-- Shows distinct (TXN_TP, MSG_TP) pairs with row count, client coverage,
-- amount stats, and zero-vs-positive amount split.
SELECT
    c.TXN_TP,
    c.MSG_TP,
    COUNT(*)                                              AS row_count,
    COUNT(DISTINCT SUBSTR(c.CLNT_CRD_NO, 7, 9))           AS distinct_clients,
    SUM(CASE WHEN c.AMT1 = 0 THEN 1 ELSE 0 END)           AS zero_amt_rows,
    SUM(CASE WHEN c.AMT1 > 0 THEN 1 ELSE 0 END)           AS pos_amt_rows,
    MIN(c.AMT1)                                           AS min_amt,
    AVG(c.AMT1)                                           AS avg_amt,
    MAX(c.AMT1)                                           AS max_amt
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV01.PT_OF_SALE_TXN c
    ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
    AND c.SRVC_CD = 36
    AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY c.TXN_TP, c.MSG_TP
ORDER BY row_count DESC
;

-- Interpretation guide (typical Visa POS codes; verify against your data):
--   TXN_TP 10 = purchase, card-present
--   TXN_TP 13 = ???  (may be online / refund / specific subset)
--   TXN_TP 11 = cash advance
--   TXN_TP 20-22 = reversal / void
--   MSG_TP '0200' / '0210' = authorization request / response
--   MSG_TP '0220' / '0230' = financial completion / response
-- Look for high-row, high-distinct-client codes with positive amounts ->
-- candidates for inclusion in the "real usage" filter.


-- Query 8 — same exploration on CLNT_CRD_POS_LOG --------------------------
-- For comparison: do these tables expose the same TXN_TP universe?
SELECT
    b.txn_tp,
    b.msg_tp,
    COUNT(*)                                              AS row_count,
    COUNT(DISTINCT SUBSTR(b.CLNT_CRD_NO, 7, 9))           AS distinct_clients,
    SUM(CASE WHEN b.AMT1 = 0 THEN 1 ELSE 0 END)           AS zero_amt_rows,
    SUM(CASE WHEN b.AMT1 > 0 THEN 1 ELSE 0 END)           AS pos_amt_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
    ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
    AND b.SRVC_CD = 36
    AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY b.txn_tp, b.msg_tp
ORDER BY row_count DESC
;


-- Query 9 — sample one row per (TXN_TP, MSG_TP) for human inspection ------
-- Useful for eyeballing what each code actually represents in your data.
SELECT
    c.TXN_TP,
    c.MSG_TP,
    c.CLNT_CRD_NO,
    c.TXN_DT,
    c.AMT1,
    c.SRVC_CD
FROM DDWV01.PT_OF_SALE_TXN c
WHERE c.SRVC_CD = 36
  AND c.TXN_DT BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.TXN_TP, c.MSG_TP ORDER BY c.TXN_DT) = 1
;
