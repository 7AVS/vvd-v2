-- =============================================================================
-- VVD SUCCESS LOGIC — FINAL
-- Final, validated success-measurement logic for the wallet-provisioning
-- campaigns. Same logic for VUT and VAW (only TACTIC_ID prefix differs).
--
-- Granularity:    (CLNT_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID)
-- Window:         success_dt in [TREATMT_STRT_DT, TREATMT_STRT_DT + 89]
-- Definitions:
--   leads          = distinct CLNT_NO in the tactic
--   primary_Nd     = distinct CLNT_NO whose EARLIEST combo first_auth_dt
--                    falls in [STRT, STRT+N-1]
--                    (1 per client max -- client-level success)
--   secondary_Nd   = COUNT of distinct (CLNT_NO, VISA_DR_CRD_NO,
--                    TOKN_REQSTR_ID) combos whose first_auth_dt falls in
--                    [STRT, STRT+N-1]
--                    (a client with multiple cards or multiple wallets
--                    contributes multiple times)
--
-- Notes / blind spots already chased and parked in DIAG_VUT_INFLATION.sql:
--   - TOKEN_LIST is a wallet-provider lookup. TOKN_REQSTR_ID identifies
--     the wallet (e.g. 40010075001 = GOOGLE PAY).
--   - "First auth in window" is treated as campaign-attributable per the
--     curated-client-list assumption (we do not look pre-STRT).
--   - VUT/VAW have the same measurement logic; deployment characteristics
--     and client selection differ but do not change the metric.
-- =============================================================================


-- VUT — final success logic ----------------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                                                        AS tactic_id,
    t.TREATMT_STRT_DT                                                                                                        AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                                       AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                                AS leads,
    COUNT(DISTINCT CASE WHEN combos.combo_first_auth <= t.TREATMT_STRT_DT + 29 THEN t.CLNT_NO END)                           AS primary_30d,
    SUM(CASE WHEN combos.combo_first_auth <= t.TREATMT_STRT_DT + 29 THEN 1 ELSE 0 END)                                       AS secondary_30d,
    COUNT(DISTINCT CASE WHEN combos.combo_first_auth <= t.TREATMT_STRT_DT + 59 THEN t.CLNT_NO END)                           AS primary_60d,
    SUM(CASE WHEN combos.combo_first_auth <= t.TREATMT_STRT_DT + 59 THEN 1 ELSE 0 END)                                       AS secondary_60d,
    COUNT(DISTINCT CASE WHEN combos.combo_first_auth IS NOT NULL              THEN t.CLNT_NO END)                           AS primary_90d,
    SUM(CASE WHEN combos.combo_first_auth IS NOT NULL              THEN 1 ELSE 0 END)                                        AS secondary_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        b.VISA_DR_CRD_NO,
        b.TOKN_REQSTR_ID,
        MIN(b.TXN_DT) AS combo_first_auth
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT,
             b.VISA_DR_CRD_NO, b.TOKN_REQSTR_ID
) combos
    ON  t.CLNT_NO         = combos.CLNT_NO
    AND t.TACTIC_ID       = combos.TACTIC_ID
    AND t.TREATMT_STRT_DT = combos.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VUT'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- VAW — final success logic ----------------------------------------------
-- Window: 30d (monthly in-app campaign).
SELECT
    TRIM(t.TACTIC_ID)                                                                                                        AS tactic_id,
    t.TREATMT_STRT_DT                                                                                                        AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                                       AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                                AS leads,
    COUNT(DISTINCT CASE WHEN combos.combo_first_auth IS NOT NULL THEN t.CLNT_NO END)                                         AS primary_30d,
    SUM(CASE WHEN combos.combo_first_auth IS NOT NULL THEN 1 ELSE 0 END)                                                     AS secondary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        b.VISA_DR_CRD_NO,
        b.TOKN_REQSTR_ID,
        MIN(b.TXN_DT) AS combo_first_auth
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT,
             b.VISA_DR_CRD_NO, b.TOKN_REQSTR_ID
) combos
    ON  t.CLNT_NO         = combos.CLNT_NO
    AND t.TACTIC_ID       = combos.TACTIC_ID
    AND t.TREATMT_STRT_DT = combos.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VAW'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- =============================================================================
-- VUI — final usage success logic (no secondary; usage campaigns have only
-- a single primary indicator)
--
-- Source table:   DDWV01.PT_OF_SALE_TXN  (matches SAS)
-- Filters:        SRVC_CD = 36, AMT1 > 0, TXN_TP IN ('10','13') (purchases)
-- Window:         [STRT, STRT+89]
--
-- Inner subquery: per (tactic, client), MIN(TXN_DT) of qualifying purchase
-- Outer aggregate: tactic x test
--   leads        = distinct CLNT_NO
--   primary_Nd   = distinct CLNT_NO whose first qualifying purchase is in
--                  [STRT, STRT+N-1]
--
-- VUI has no secondary metric. The "usage" measurement that secondary on
-- VCN/VDA will eventually borrow is this same primary logic.
-- =============================================================================

-- VUI — final usage success logic ----------------------------------------
-- Window: 30d (triggered usage campaign). Primary only.
SELECT
    TRIM(t.TACTIC_ID)                                                                                           AS tactic_id,
    t.TREATMT_STRT_DT                                                                                           AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                          AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                   AS leads,
    COUNT(DISTINCT CASE WHEN c.first_txn_dt IS NOT NULL THEN t.CLNT_NO END)                                     AS primary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) c
    ON  t.CLNT_NO         = c.CLNT_NO
    AND t.TACTIC_ID       = c.TACTIC_ID
    AND t.TREATMT_STRT_DT = c.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VUI'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- =============================================================================
-- VCN / VDA — final acquisition success logic
--
-- Primary:   client got a NEW VVD card ISSUED during the campaign window
--            (DDWV01.VISA_DR_CRD_DLY, ISS_DT in [STRT, STRT+89])
--            Note: ISS_DT, NOT ACTV_DT. Activation is the VDT campaign metric.
-- Secondary: client used the card (purchase) during the campaign window.
--            Borrows the locked VUI usage logic verbatim:
--            DDWV01.PT_OF_SALE_TXN, AMT1 > 0, TXN_TP IN ('10','13'), SRVC_CD=36
--
-- Same measurement logic for VCN and VDA (both acquisition campaigns).
-- Only the TACTIC_ID prefix differs.
--
-- Output: tactic_id | treatmt_strt_dt | test | leads
--         | primary_30d   | secondary_30d
--         | primary_60d   | secondary_60d
--         | primary_90d   | secondary_90d
-- =============================================================================


-- VCN — final acquisition success ----------------------------------------
-- Window: 30d (monthly campaign). Primary only — SAS has no secondary.
SELECT
    TRIM(t.TACTIC_ID)                                                                                                AS tactic_id,
    t.TREATMT_STRT_DT                                                                                                AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                               AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                        AS leads,
    COUNT(DISTINCT CASE WHEN prim.first_iss_dt IS NOT NULL THEN t.CLNT_NO END)                                       AS primary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) prim
    ON  t.CLNT_NO         = prim.CLNT_NO
    AND t.TACTIC_ID       = prim.TACTIC_ID
    AND t.TREATMT_STRT_DT = prim.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VCN'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- VDA — final acquisition success ----------------------------------------
SELECT
    TRIM(t.TACTIC_ID)                                                                                                AS tactic_id,
    t.TREATMT_STRT_DT                                                                                                AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                               AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                        AS leads,
    COUNT(DISTINCT CASE WHEN prim.first_iss_dt <= t.TREATMT_STRT_DT + 29 THEN t.CLNT_NO END)                         AS primary_30d,
    COUNT(DISTINCT CASE WHEN sec.first_txn_dt  <= t.TREATMT_STRT_DT + 29 THEN t.CLNT_NO END)                         AS secondary_30d,
    COUNT(DISTINCT CASE WHEN prim.first_iss_dt <= t.TREATMT_STRT_DT + 59 THEN t.CLNT_NO END)                         AS primary_60d,
    COUNT(DISTINCT CASE WHEN sec.first_txn_dt  <= t.TREATMT_STRT_DT + 59 THEN t.CLNT_NO END)                         AS secondary_60d,
    COUNT(DISTINCT CASE WHEN prim.first_iss_dt IS NOT NULL              THEN t.CLNT_NO END)                          AS primary_90d,
    COUNT(DISTINCT CASE WHEN sec.first_txn_dt  IS NOT NULL              THEN t.CLNT_NO END)                          AS secondary_90d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
        MIN(card.ISS_DT) AS first_iss_dt
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
) prim
    ON  t.CLNT_NO         = prim.CLNT_NO
    AND t.TACTIC_ID       = prim.TACTIC_ID
    AND t.TREATMT_STRT_DT = prim.TREATMT_STRT_DT
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
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
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) sec
    ON  t.CLNT_NO         = sec.CLNT_NO
    AND t.TACTIC_ID       = sec.TACTIC_ID
    AND t.TREATMT_STRT_DT = sec.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VDA'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


-- =============================================================================
-- VDT — final activation success logic
--
-- Primary:   client activated their (issued, previously inactive) VVD card
--            during the campaign window
--            (DDWV01.VISA_DR_CRD_DLY, ACTV_DT in [STRT, STRT+89])
--            ACTV_DT is correct for VDT (campaign IS about activation).
--            VCN/VDA use ISS_DT instead because they measure acquisition.
-- Secondary: none. VDT is single-indicator. The dashboard SAS likely has a
--            secondary (per the `SUCCESS_IND_1` naming convention seen in
--            the photographed portion of VDT_activation.sas) but the
--            corresponding sections are not yet photographed.
--
-- Brand filter (`VISA_DR_CRD_BRND_CD = '01'`) used by the dashboard SAS is
-- NOT applied here. The VDT tactic list is pre-filtered to clients with
-- inactive cards needing activation; the brand restriction is redundant.
-- The field's value mapping is also not authoritatively verified in either
-- the Vintage pipeline (`add_card_type` flag is dead config across
-- vintage_engine v2.3-v2.7) or the EDW data dictionary.
-- See success_logic_validation.md §9.6 for full reasoning.
--
-- Output: tactic_id | treatmt_strt_dt | test | leads
--         | primary_30d | primary_60d | primary_90d
-- =============================================================================


-- VDT — final activation success -----------------------------------------
-- Window: 30d (triggered campaign — day 7 + day 15 from issuance). Primary only.
SELECT
    TRIM(t.TACTIC_ID)                                                                                                AS tactic_id,
    t.TREATMT_STRT_DT                                                                                                AS treatmt_strt_dt,
    TRIM(t.TST_GRP_CD)                                                                                               AS test,
    COUNT(DISTINCT t.CLNT_NO)                                                                                        AS leads,
    COUNT(DISTINCT CASE WHEN prim.first_actv_dt IS NOT NULL THEN t.CLNT_NO END)                                      AS primary_30d
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
LEFT JOIN (
    SELECT
        a.CLNT_NO,
        a.TACTIC_ID,
        a.TREATMT_STRT_DT,
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
    GROUP BY a.CLNT_NO, a.TACTIC_ID, a.TREATMT_STRT_DT
) prim
    ON  t.CLNT_NO         = prim.CLNT_NO
    AND t.TACTIC_ID       = prim.TACTIC_ID
    AND t.TREATMT_STRT_DT = prim.TREATMT_STRT_DT
WHERE substr(t.TACTIC_ID, 8, 3) = 'VDT'
  AND t.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
