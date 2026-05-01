# Success Logic Validation — VCN, VDA, VDT, VUI, VUT, VAW

**Status:** locked 2026-05-01. Source of truth for the success measurement of all 6 VVD campaigns.

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

### 3.2 VDT — activation (not formally validated this round)
Existing logic in `VVD_SUMMARY_FIXED_WINDOW.sql` uses `ACTV_DT`. This is the correct field for VDT (campaign IS about activation). No changes made.

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

VUI omits the `secondary_*` columns (single-indicator).

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
