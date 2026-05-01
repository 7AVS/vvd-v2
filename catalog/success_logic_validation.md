# Success Logic Validation — VCN, VDA, VDT, VUI, VUT, VAW

**Status:** locked 2026-05-01. Consolidation pass: 2026-05-01.
Source of truth for the success measurement of all 6 VVD campaigns.

This document catalogues the validation work done against the dashboard team's SAS pipeline and locks the final Teradata logic for each campaign type. Future sessions should start here.

---

## 1. Files

### Final implementation
- **`edw_sql/VVD_SUCCESS_LOGIC_FINAL.sql`** — locked logic for all 6 campaigns. Independent queries, runnable in Teradata, same output shape per campaign type.

### Diagnostic / exploration
- `edw_sql/DIAG_VUT_INFLATION.sql` — proved that secondary inflation on VUT/VAW was recurring keep-alive auths; established `(CLNT_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID)` as the correct provisioning grain.
- `edw_sql/DIAG_VUI.sql` — compared `PT_OF_SALE_TXN` vs `CLNT_CRD_POS_LOG` for VUI usage; explored `TXN_TP` / `MSG_TP` distributions.

### Legacy / cross-validation only (do not modify)
- `edw_sql/VVD_SUMMARY_FIXED_WINDOW.sql` — prior version, kept for cross-validation
- `edw_sql/VVD_SUMMARY.sql` — flat 8-campaign summary using `TREATMT_END_DT`
- `edw_sql/VCN.sql`, `VDA.sql`, `VDT.sql`, `VUI.sql`, `VUT.sql`, `VAW.sql`, `IPC.sql`, `IRI.sql` — original per-campaign vintage curve queries
- `edw_sql/VUT_VAW_SAS_REPLICA.sql` — direct port of SAS pipeline; superseded by FINAL

### Dashboard team's SAS reference (photographed; transcribed below)
- `VUT_VAW_Provisioning.sas`
- `VCN_VDA_Acquisition.sas`
- `VUI_Usage.sas`
- `VDT_activation.sas` (not yet reviewed)

---

## 2. Source tables (all Teradata / EDW)

| Table | Purpose |
|---|---|
| `DG6V01.TACTIC_EVNT_IP_AR_HIST` | Campaign population (clients targeted by tactic) |
| `DDWV01.VISA_DR_CRD_DLY` | Daily Visa Debit card snapshot (issuance, activation, status) — **NOT** `_DIY` (was a typo in earlier code) |
| `DDWV01.PT_OF_SALE_TXN` | Point-of-sale transactions — **DDWV01 NOT DDWV05** (corrected) |
| `DDWV05.CLNT_CRD_POS_LOG` | Client card POS log — used for wallet provisioning auths |
| `DL_DECMAN.TOKEN_LIST` | Wallet-provider lookup. **Only 13 wallet-flagged rows total.** `TOKEN_ID` = wallet type, not unique per provisioning |
| `DTZV01.TACTIC_EVNT_IP_AR_H60M` | IMT campaign population (IPC, IRI — non-VVD) |
| `DDWV01.EXT_CDP_CHNL_EVNT` | IMT success events (non-VVD) |

---

## 3. Final per-campaign logic (LOCKED)

All campaigns share the population scope:
```
WHERE substr(a.TACTIC_ID, 8, 3) = '<MNE>'
  AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
```

Window for all success measures: `[STRT, STRT + 89]` with milestone breakdowns at `+29 (30d)`, `+59 (60d)`, `+89 (90d)`.

### 3.1 VCN, VDA — acquisition

**Primary** = client got a NEW VVD card ISSUED in window.
```sql
INNER JOIN DDWV01.VISA_DR_CRD_DLY card
    ON a.CLNT_NO = card.CLNT_NO
    AND card.STS_CD IN ('06', '08')
    AND card.SRVC_ID = 36
    AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
    AND card.ISS_DT BETWEEN STRT AND (STRT + 89)
```
- **`ISS_DT`** (issue date), NOT `ACTV_DT`. Activation is the VDT metric. The SAS dashboard uses `ACTV_DT` — **the SAS is wrong** by the project's definition.
- Reason: VVD has 3 card types — fully digital (auto-activated at issuance), plastic, hybrid. Only fully digital activates immediately; using `ACTV_DT` for acquisition systematically under-counts non-digital clients.
- Granularity: 1 per client (`COUNT DISTINCT CLNT_NO`).

**Secondary** = client used the card (purchase) in window. Borrows VUI primary verbatim (see 3.3).

### 3.2 VDT — activation

**Primary** = client activated their (issued, previously inactive) VVD card during the window.
```sql
INNER JOIN DDWV01.VISA_DR_CRD_DLY card
    ON a.CLNT_NO = card.CLNT_NO
    AND card.STS_CD IN ('06', '08')
    AND card.SRVC_ID = 36
    AND card.ACTV_DT IS NOT NULL
    AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
    AND card.ACTV_DT BETWEEN STRT AND (STRT + 89)
```
- **`ACTV_DT`** (activation date) is correct for VDT. VDT IS the activation campaign — measuring whether issued-but-inactive cards get activated within the window.
- VDT tactic population is pre-filtered upstream to clients-with-inactive-cards (campaign mechanic: 7-day post-issuance email trigger + 7-day reminder). No additional brand filter is applied — see §9.6.
- Granularity: 1 per client (`COUNT DISTINCT CLNT_NO`).

**Secondary** = none. VDT is single-indicator. Dashboard SAS likely has a secondary block but it is not yet photographed (see §9.6).

### 3.3 VUI — usage (PRIMARY ONLY, no secondary)

**Primary** = client made at least one settled Visa Debit purchase in window.
```sql
INNER JOIN DDWV01.PT_OF_SALE_TXN c
    ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
    AND c.SRVC_CD = 36
    AND c.MSG_TP = '0220'                                -- settlement message
    AND c.AMT1 > 0
    AND (
             (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
          OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
        )
    AND c.TXN_DT BETWEEN STRT AND (STRT + 89)
```
- **Filter is per the EDW data dictionary** for `PT_OF_SALE_TXN.TXN_TP` (validated 2007). This is the official "Visa Debit purchase" definition.
- `MSG_TP='0220'` = financial completion / clearing (the transaction settled). NOT `'0210'` (auth response — what the SAS uses, which over-counts because auths can fail to settle).
- `RCNCL_REAS_CD IN ('M','S')` = matched / settled reason codes.
- Refund-reversal branch (`TXN_TP=22` with `RCNCL_REAS_CD IN ('P','E')`) catches edge case where a refund was reversed → net effect is a debit / purchase.
- For SRVC_CD=36, valid TXN_TPs are only `10, 13, 14, 21, 22`. Of these, **only 10 and 13 are purchases.** 14=refund, 21=purchase reversal, 22=refund reversal. Confirmed against EDW data dictionary.

**Secondary** = none. VUI is single-indicator by design.

### 3.4 VUT, VAW — wallet provisioning

**Granularity:** `(CLNT_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID)` — one combo per unique card-in-wallet provisioning.

**Primary** = 1 per client whose earliest combo `first_auth_dt` is in `[STRT, STRT+N-1]`.

**Secondary** = count of distinct combos whose `first_auth_dt` is in window.
- A client with 1 card in 2 wallets → 2.
- A client with 2 cards in 1 wallet → 2.
- A client whose existing token just keeps pinging → 1 (recurring auths collapsed by `MIN(TXN_DT)` per combo).

**Source:** `DDWV05.CLNT_CRD_POS_LOG` (zero-dollar wallet auths) joined to `DL_DECMAN.TOKEN_LIST` filtered to `TOKEN_WALLET_IND='Y'`. Filter set:
```sql
AND b.SRVC_CD = 36
AND b.AMT1 = 0
AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
AND b.APPROVAL_CODE IS NOT NULL
```

---

## 4. Output shape (all campaigns)

```
tactic_id | treatmt_strt_dt | test | leads | primary_30d | secondary_30d
                                          | primary_60d | secondary_60d
                                          | primary_90d | secondary_90d
```

VUI and VDT omit the `secondary_*` columns (single-indicator). VDT may add a secondary once the dashboard SAS tail is photographed (see §9.6).

---

## 5. Critical data facts learned this validation round

1. **`DDWV01.VISA_DR_CRD_DLY`** — daily, NOT `_DIY` (project memory had this fixed but flagging again here).
2. **`DDWV01.PT_OF_SALE_TXN`** — in `DDWV01`, NOT `DDWV05`. The SAS we transcribed showed `DDWV05` but the user clarified the actual library is `DDWV01`.
3. **`TOKEN_LIST` is a 13-row lookup.** Wallet provider type, not per-token. `TOKEN_ID = 40010075001` = `GOOGLE PAY`. Same `TOKEN_ID` shared across all clients/cards using that wallet.
4. **Recurring zero-dollar auths inflate naive POS counts ~6×.** Wallet tokens send periodic keep-alive auths to the issuer; these match the same filter set as the initial provisioning. Use `MIN(TXN_DT) per (card, wallet)` to identify the actual provisioning event.
5. **`MSG_TP` codes:** `0200`/`0210` = auth request/response. `0220`/`0230` = financial completion / response. **The SAS uses `0210` (auth) for VDA secondary — wrong; should be `0220` (settlement).**
6. **For SRVC_CD=36, valid TXN_TP codes are 10, 13, 14, 21, 22 only.** 10/13 are the only purchases.
7. **Visa cards on this product use BIN prefixes `45190` and `45199`.** Both must be checked for wallet provisioning identification.

---

## 6. Decisions parked (do NOT chase further)

1. **Pre-campaign provisioning detection.** Whether a client was already tokenized before the campaign window. Curated client list assumption: if a client is in the contact list, they didn't have the wallet pre-campaign. Trust the curation; don't widen lookback.
2. **Initial vs keep-alive auth discriminator.** No field in `CLNT_CRD_POS_LOG` distinguishes them directly. The `(card, wallet, MIN(TXN_DT))` collapse is the practical answer.
3. **Card-anomaly cases.** A second `VISA_DR_CRD_NO` for the same client could be reissue, device change, new wallet app. Not knowable from this data, doesn't change the metric.
4. **VUT 30d vs 90d window discrepancy.** Doc says 30d, vintage SQL says 90d. Final uses STRT+89 across the board for consistent 30/60/90 milestones. Not material.
5. **`TREATMT_END_DT + 30` SAS buffer.** Flagged; adds 30d to back-end window. Not adopted because we use explicit `STRT+89` instead.

---

## 7. SAS pipeline summary (for reference)

The dashboard team's SAS uses a 3-step pattern per campaign:
1. **`*_TACTIC`** — pull campaign population from `TACTIC_EVNT_IP_AR_HIST`.
2. **`*_success01`** (and optional `*_success02`) — INNER JOIN to success table, `1 AS SUCCESS_IND`.
3. **`*_SUMM`** / **`*_CAMPAIGN_DATA`** — `LEFT JOIN tactic to success01/02`, `COALESCE(SUCCESS_IND, 0)`. Roll up via `proc freq`.

### SAS bugs / discrepancies found this round
| Campaign | SAS does | Project says | Why |
|---|---|---|---|
| VCN/VDA | `B.ACTV_DT` in window | **`B.ISS_DT`** in window | Activation is VDT's metric; for acquisition, issuance is the proxy. Mixing under-counts non-digital cards. |
| VUT/VAW | `SUM(rows)` for "secondary" | `COUNT(distinct combos)` | Recurring keep-alive auths inflate ~6×. |
| VDA secondary, VUI | `MSG_TP='0210'` (auth) or no MSG_TP filter | **`MSG_TP='0220'` + reason codes** | Auths can fail to settle. Dictionary requires settlement message and reconciliation reason. |
| VUI | AMT/TXN_TP filters commented out | **filters required** | Commenting was a bug, not intentional broadening. |

### SAS code excerpts (transcribed from photos)

#### VCN/VDA primary (success01)
```sql
SELECT DISTINCT A.CLNT_NO, A.TACTIC_ID, A.TST_GRP_CD, A.rpt_grp_cd,
       A.TREATMT_MN, 1 AS SUCCESS1_IND
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A
INNER JOIN DDWV01.VISA_DR_CRD_DLY AS B
  ON A.CLNT_NO = B.CLNT_NO
  AND substr(a.tactic_id,8,3) = &mne.
  AND B.STS_CD IN ('06', '08')
  AND B.ACTV_DT BETWEEN A.TREATMT_STRT_DT AND A.TREATMT_END_DT  -- WRONG: should be ISS_DT
  AND B.SNAP_DT = &me_dt.
```

#### VDA secondary (success02) — first purchase
```sql
SELECT DISTINCT c.clnt_no, tactic.tactic_id, ..., 1 AS SUCCESS2_IND
FROM DDWV01.PT_OF_SALE_TXN AS c, DG6V01.TACTIC_EVNT_INFO_HIST AS tactic
WHERE c.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND tactic.TREATMT_END_DT
  AND c.SRVC_CD = 36
  AND c.amt1 > 0
  AND c.txn_tp IN ('10','13')
  AND c.MSG_TP = '0210'                          -- WRONG: should be '0220'
  AND tactic.clnt_no = SUBSTR(c.clnt_crd_no,7,9)
```

#### VUI primary
```sql
SELECT DISTINCT tactic.clnt_no, tactic.tactic_id, ..., 1 AS SUCCESS_IND
FROM DDWV01.PT_OF_SALE_TXN AS C, DG6V01.TACTIC_EVNT_INFO_HIST AS tactic
WHERE C.TXN_DT BETWEEN tactic.TREATMT_STRT_DT AND (tactic.TREATMT_STRT_DT + 90)
  AND C.SRVC_CD IN (36)
  /* AND c.amt1 > 0 */                            -- COMMENTED OUT (bug)
  /* AND c.txn_tp IN ('10','13') */               -- COMMENTED OUT (bug)
  AND substr(tactic.tactic_id,8,12) = 'VUI'
  AND tactic.clnt_no = SUBSTR(C.clnt_crd_no,7,9)
```

#### VUT/VAW provisioning (success01)
```sql
SELECT A.CLNT_NO, A.TACTIC_ID, ..., a.TREATMT_END_DT + 30 AS new_end_dt,
       MIN(txn_dt) AS min_dt
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A
INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B
  ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO, 7, 9)
INNER JOIN DL_DECMAN.TOKEN_LIST AS C
  ON B.TOKN_REQSTR_ID = C.TOKEN_ID
WHERE B.TXN_DT BETWEEN date_strt. AND date_end_.
  AND B.AMT1 = 0
  AND SUBSTR(B.CLNT_CRD_NO, 1, 5)  = '45190'
  AND SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'
  AND SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'
  AND B.POS_ENTR_MODE_CD_NON_EMV = '000'
  AND C.TOKEN_WALLET_IND = 'Y'
  AND B.SRVC_CD = 36
  AND A.tactic_id = &tactic.
GROUP BY 1,2,3,4,5,6,7
```

---

## 8. EDW data dictionary — `PT_OF_SALE_TXN.TXN_TP` (validated 2007)

**Service Code 36 (Visa Debit) valid TXN_TP values:**
- `10` = Visa Debit Purchase
- `13` = Visa Debit Purchase (likely card-not-present)
- `14` = Refund
- `21` = Purchase reversal
- `22` = Refund reversal

**Official "Visa Debit purchase" identification logic** (from the dictionary's Usage Note):
```sql
COUNT(CASE WHEN
       (MSG_TP = '0220' AND RCNCL_REAS_CD IN ('M','S'))
    OR (MSG_TP = '0220' AND TXN_TP = '22' AND RCNCL_REAS_CD IN ('P','E'))
THEN AMT1 ELSE NULL END)
```
This is what we adopted in the FINAL file.

---

## 9. Dashboard SAS consolidation (2026-05-01)

Full reconciliation detail in `references/dashboard_sas/RECONCILIATION.md`. This section records the per-campaign verdict and the rationale. The VVD SQL is the winner in every resolved case — all blocking divergences are bugs in the SAS, not in our SQL.

---

### 9.1 VCN — VERDICT: REJECT

**Dashboard SAS does:** Primary success = `ACTV_DT BETWEEN TREATMT_STRT_DT AND TREATMT_END_DT` on `DDWV01.VISA_DR_CRD_DLY` with a hardcoded snapshot pin (`SNAP_DT = '2024-06-30'`); tactic filter is a single literal (`'2024001VCN'`).

**Our locked SQL does:** Primary success = `ISS_DT BETWEEN STRT AND (STRT + 89)` on the same table; snapshot pin = `(SELECT MAX(SNAP_DT) ...)` (always latest); tactic filter = `substr(TACTIC_ID, 8, 3) = 'VCN'` covering all deployments.

**Why we differ:**
- `ACTV_DT` is the VDT metric (activation). VCN's goal is card acquisition — the observable proxy is issuance, not activation. Digital cards auto-activate at issuance, so `ACTV_DT = ISS_DT` for them; but non-digital cards may activate days or weeks later, causing `ACTV_DT` to fall outside the window and go uncounted. `ISS_DT` is the correct event.
- Hardcoded `SNAP_DT = '2024-06-30'` means cards issued after that snapshot date are invisible. `MAX(SNAP_DT)` always reflects the current state of the card register.
- Single-tactic literal locks the SAS to one deployment cycle. Our `SUBSTR` pattern is reusable across all VCN tactics.

**Nothing in the SAS is worth absorbing.** Our SQL stays as-is.

---

### 9.2 VDA — VERDICT: REJECT

**Dashboard SAS does:** Primary success = `ACTV_DT BETWEEN TREATMT_STRT_DT AND TREATMT_END_DT` (same bug as VCN). Secondary success = first purchase using `MSG_TP = '0210'` (auth response) with no `RCNCL_REAS_CD` filter; secondary pulls from `TACTIC_EVNT_INFO_HIST` for both tactic and transaction history.

**Our locked SQL does:** Primary = `ISS_DT BETWEEN STRT AND (STRT + 89)`. Secondary = first settled purchase using `MSG_TP = '0220'` + `RCNCL_REAS_CD IN ('M','S')` (or refund-reversal branch); all from `TACTIC_EVNT_IP_AR_HIST`.

**Why we differ:**
- `ACTV_DT` is wrong for VDA for the same reason as VCN (see §9.1).
- `MSG_TP = '0210'` is the auth response — the issuer's reply to a payment attempt. It is emitted even when the auth is declined or the transaction never settles. Using `'0210'` inflates the secondary purchase count by including failed authorizations. `'0220'` is the financial completion message, confirming the transaction settled.
- Without `RCNCL_REAS_CD`, reversals and non-standard clearing paths contaminate the purchase signal.
- SAS uses `TACTIC_EVNT_INFO_HIST` for the secondary query while using `TACTIC_EVNT_IP_AR_HIST` for the primary — an inconsistency with no documented justification. Our SQL uses `IP_AR_HIST` for both.

**Nothing in the SAS is worth absorbing.** Our SQL stays as-is.

---

### 9.3 VUI — VERDICT: REJECT

**Dashboard SAS does:** Purchase success with `AMT1 > 0` commented out (line 80), no `MSG_TP` filter, no `RCNCL_REAS_CD` filter, `TXN_TP` values as unquoted integers. Uses `TACTIC_EVNT_INFO_HIST`. Date range is hardcoded `'2024-08-19'` to `'2024-10-17'`. Additionally, the final rollup join at SAS line 124 has a critical syntax bug: `AND A.TACTIC_ID AND B.TACTIC_ID` — missing the `=` operator, making it a boolean existence test rather than an equality join (effectively a cross-join or error in Teradata).

**Our locked SQL does:** `AMT1 > 0` required; `MSG_TP = '0220'`; `RCNCL_REAS_CD IN ('M','S')` required; `TXN_TP IN ('10','13')` as strings; all from `TACTIC_EVNT_IP_AR_HIST`; date range `'2025-01-01'` to `'2026-03-31'`.

**Why we differ:**
- Commented-out `AMT1 > 0` allows zero-value transactions: wallet keep-alive auths, test transactions, declined-but-logged entries. These are not purchases. The comment was a debugging artifact left in production code, not an intentional design choice.
- Without `MSG_TP = '0220'`, auth requests that never settled (declined, timed out) are counted as successful purchases.
- Without `RCNCL_REAS_CD`, reversal transactions contaminate the count.
- The line 124 join bug (`AND A.TACTIC_ID AND B.TACTIC_ID` instead of `AND A.TACTIC_ID = B.TACTIC_ID`) means the SAS rollup table is structurally broken regardless of whether the success filters were correct.

**Nothing in the SAS is worth absorbing.** Our SQL stays as-is.

---

### 9.4 VUT — VERDICT: REJECT

**Dashboard SAS does:** Wallet provisioning with an absolute hardcoded date window (`func_strt = '2024-07-20'` to `camp_end = '2024-09-18'`), BIN filter `SUBSTR(B.CLNT_CRD_NO, 1, 9) = '45190'` (9-char match against a 5-digit BIN — will never match), no `APPROVAL_CODE` guard, single tactic literal `'2024232VUT'`, and a `TREATMT_END_DT + 30` back-end buffer with a secondary primary indicator checked against `STRT + 30`.

**Our locked SQL does:** `TXN_DT BETWEEN STRT AND (STRT + 89)` (tactic-relative); `VISA_DR_CRD_NO LIKE '45190%' OR LIKE '45199%'` (correct prefix match); `APPROVAL_CODE IS NOT NULL` (anti-inflation guard); `substr(TACTIC_ID, 8, 3) = 'VUT'` (all tactics); granularity = `(CLNT_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID)`.

**Why we differ:**
- Hardcoded absolute dates freeze the query to a single campaign run and prevent reuse across VUT redeployments.
- `SUBSTR(B.CLNT_CRD_NO, 1, 9) = '45190'` extracts 9 characters and compares to a 5-character string — this comparison is always false in Teradata (padding mismatch). The BIN filter likely zeros out the entire VUT result set in the SAS. `LIKE '45190%'` is the correct pattern.
- `APPROVAL_CODE IS NOT NULL` is the primary guard against counting recurring keep-alive auths as new provisioning events. Without it, a single provisioned card generates ~6× inflated counts from periodic wallet pings (validated in `DIAG_VUT_INFLATION.sql`).
- The undocumented `+30d` back-end buffer (`TREATMT_END_DT + 30`) extends the success window beyond what was intended and is inconsistent with the `STRT+89` framework used across all campaigns. Not adopted — see §6 item 5.

**Nothing in the SAS is worth absorbing.** Our SQL stays as-is.

---

### 9.5 VAW — VERDICT: REJECT

**Dashboard SAS does:** Wallet provisioning via `DG6V01.CLNT_CRD_POS_LOG` (wrong library) and `DI_DECMAN.TOKEN_LIST` (wrong library prefix); `TOKEN_WALLET_IND = 'V'`; `SRVC_CD = 38`; pre-window lookback `TXN_DT BETWEEN TREATMT_STRT_DT - 30 AND TREATMT_END_DT`; no `APPROVAL_CODE` guard; `VISA_OR_CRD_NO` (suspected field name typo). Secondary block uses `substr(TACTIC_ID, 8, 12)` (length 12 — produces `'VAW         '`, never matches `'VAW'`).

**Our locked SQL does:** `DDWV05.CLNT_CRD_POS_LOG`; `DL_DECMAN.TOKEN_LIST`; `TOKEN_WALLET_IND = 'Y'`; `SRVC_CD = 36`; `TXN_DT BETWEEN STRT AND (STRT + 89)` (no lookback); `APPROVAL_CODE IS NOT NULL`; `VISA_DR_CRD_NO` (correct field name); `substr(TACTIC_ID, 8, 3) = 'VAW'`.

**Why we differ:**
- `DG6V01` is the tactic event history library, not the POS log library. The POS log lives in `DDWV05`. Using the wrong library would cause a table-not-found error or return empty results.
- `DI_DECMAN` vs `DL_DECMAN` is a library prefix error on `TOKEN_LIST`. `DL_DECMAN` is confirmed correct.
- `TOKEN_WALLET_IND = 'V'` produces zero matches — the only valid value in the `TOKEN_LIST` wallet flag column is `'Y'`. The `'V'` is a transcription error.
- `SRVC_CD = 38` is not Visa Debit. `SRVC_CD = 36` is the correct Visa Debit service code; zero-dollar wallet provisioning auths for VVD cards carry `SRVC_CD = 36`.
- Pre-window lookback (`STRT - 30`) attributes pre-campaign provisioning events to campaign success, violating the curated-client-list assumption (§6 item 1).
- `substr(TACTIC_ID, 8, 12)` extracts 12 characters into a right-padded string, which will never match the 3-character literal `'VAW'` in Teradata. The secondary block silently returns zero rows.
- `APPROVAL_CODE IS NOT NULL` is missing — same inflation risk as VUT (§9.4).

VAW had the highest number of blocking divergences of any campaign (6). Several bugs would individually produce zero results. **Nothing in the SAS is worth absorbing.** Our SQL stays as-is.

---

### 9.6 VDT — VERDICT: CONSOLIDATED (SAS rejected; FINAL adopts existing window-summary logic)

The dashboard SAS for VDT (`VDT_activation.sas`) is photographed only through the `_p_success_&mnemonic` collect-stats line. The photographed portion contains the full success-detection block (`P_SUCCESS_TEMP`); the TACTIC pull and proc freq output sections remain not photographed. The success criteria is the only part that affects the FINAL SQL, so consolidation proceeded on what is available.

**Two existing inputs compared side-by-side:**

| Filter | `VVD_SUMMARY_FIXED_WINDOW.sql` (ours) | Dashboard SAS `P_SUCCESS_TEMP` | FINAL decision |
|---|---|---|---|
| Source table | `DDWV01.VISA_DR_CRD_DLY` | `DDWV01.VISA_DR_CRD_DLY` | same — keep |
| Join key | `a.CLNT_NO = b.CLNT_NO` | `VISA_DR_CRD_DLY.AR_ID = unq_mne_clnt.AR_ID` | **`CLNT_NO`** — matches VCN/VDA. AR_ID is account-level (more granular) but inconsistent with the rest of the FINAL file. Flag for future. |
| Success date | `ACTV_DT` | `ACTV_DT` | same — VDT IS the activation campaign |
| Window | `BETWEEN STRT AND STRT+89` (90 days) | `BETWEEN STRT AND STRT+90` (91 days) | **`+89`** — matches the 30/60/90 milestone framework used in every other FINAL block |
| `STS_CD` | `IN ('06','08')` | `IN ('06')` | **`('06','08')`** — matches VCN/VDA. Once-active cards count even if subsequently closed |
| `SRVC_ID = 36` | YES | NO | **YES** — matches VCN/VDA, identifies VVD service |
| `ACTV_DT IS NOT NULL` | YES | implicit via BETWEEN | **YES** — defensive |
| `SNAP_DT` pin | `MAX(SNAP_DT)` | `MAX(SNAP_DT) WHERE > CURRENT_DATE - 5` | **`MAX(SNAP_DT)`** — functionally equivalent, simpler |
| `CRD_CNTRL_CD = '0'` (not blocked) | NO | YES | **REJECT** — see below |
| `VISA_DR_CRD_BRND_CD = '01'` (brand) | NO | YES | **REJECT** — see below |

**`CRD_CNTRL_CD = '0'` rejected.** `'0'` means no control/restriction on the card. Adding this filter would exclude cards that were activated and then later blocked (lost/stolen/fraud hold). The activation event still occurred — a subsequent block does not undo it. VCN/VDA also do not apply this filter; adding it only to VDT would be inconsistent without justification.

**`VISA_DR_CRD_BRND_CD = '01'` rejected.** The dashboard team interprets `'01'` as "non-digital" (the cards that need activation, since fully digital cards auto-activate at issuance). The reasoning is plausible — VDT does target inactive non-digital cards. However:
- The VDT tactic list is already pre-filtered upstream to clients-with-inactive-cards (the campaign mechanic — 7-day post-issuance trigger). The brand restriction is redundant on top of that population scope.
- The field's value mapping is **not authoritatively documented**. The Vintage pipeline (`vintage_engine.py` v2.5, with versions v2.3, v2.4, v2.6, v2.7 also checked) declares an `add_card_type: True` flag for `card_acquisition` and `card_activation` but **never consumes it** — no `withColumn` mapping codes to digital/non-digital labels exists in any version. Memory previously recorded `'03' = Digital` based on distribution, but no code or dictionary entry confirms `'01' = non-digital`.
- Applying an unverified filter that affects ~40% of records (the share of cards with `BRND_CD = '01'`) carries large measurement risk. If `'01'` is something else, VDT success is materially under-counted.

The dashboard team's choice to filter by brand may be correct for their use case, but is not adopted into FINAL until the field's value mapping is verified. **Open question parked.** A future verification pass should: (a) check the EDW data dictionary entry for `VISA_DR_CRD_BRND_CD`, (b) join to a known digital/plastic indicator (e.g. card-form-factor field), and (c) decide whether VCN/VDA also need the filter.

**Secondary metric.** The dashboard SAS likely has a secondary block (per the `SUCCESS_IND_1` field naming in the photographed portion — the `_1` suffix only appears when `_2` exists). The corresponding photos are not yet available. FINAL omits a secondary indicator for VDT (matches the VUI pattern). Add when SAS is fully photographed.

**FINAL VDT block** is in `VVD_SUCCESS_LOGIC_FINAL.sql`. It is structurally identical to the existing `VVD_SUMMARY_FIXED_WINDOW.sql` block, reformatted to match the per-campaign subquery style of VCN/VDA/VUI/VUT/VAW.

---

### 9.7 Consolidation summary

| Campaign | Verdict | SQL changes required |
|---|---|---|
| VCN | REJECT | None — SAS has 2 blocking bugs (ACTV_DT, hardcoded snapshot). Our SQL is correct. |
| VDA | REJECT | None — SAS has 3 blocking bugs (ACTV_DT, MSG_TP='0210', no RCNCL_REAS_CD). Our SQL is correct. |
| VUI | REJECT | None — SAS has 3 blocking bugs (AMT1 commented out, no MSG_TP, no RCNCL_REAS_CD) plus a fatal join syntax error on line 124. Our SQL is correct. |
| VUT | REJECT | None — SAS has 4 blocking bugs (absolute dates, broken BIN SUBSTR, no APPROVAL_CODE, wrong granularity). Our SQL is correct. |
| VAW | REJECT | None — SAS has 6 blocking bugs (wrong libraries, wrong SRVC_CD, wrong TOKEN_WALLET_IND, pre-window lookback, no APPROVAL_CODE, broken SUBSTR length). Our SQL is correct. |
| VDT | REJECT (consolidated) | Added to FINAL using existing window-summary logic. SAS adds two filters (`CRD_CNTRL_CD`, `VISA_DR_CRD_BRND_CD`) — both rejected (see §9.6). SAS likely has a secondary block but it is not yet photographed; FINAL is single-indicator like VUI. |
