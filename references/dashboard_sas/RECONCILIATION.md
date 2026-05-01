# VVD Dashboard SAS vs. VVD SQL — Reconciliation Report

**Date:** 2026-05-01
**Source of truth:** `edw_sql/VVD_SUCCESS_LOGIC_FINAL.sql` + `catalog/success_logic_validation.md` (locked 2026-05-01)
**SAS files reviewed:** vcn.sas, vda.sas, vui.sas, vut.sas, vaw.sas (vdt.sas excluded — handled separately)

---

## Campaign: VCN (Contextual Notification — card acquisition)

**Dashboard SAS approach**
- Tactic filter: `tactic_id = '2024001VCN'` (single hardcoded tactic, line 34 / 57)
- Success table: `DDWV01.VISA_DR_CRD_DLY` joined on `A.CLNT_NO = B.CLNT_NO`
- Success criteria: `B.STS_CD IN ('06', '08')` AND **`B.ACTV_DT BETWEEN A.TREATMT_STRT_DT AND A.TREATMT_END_DT`** (line 60)
- Snapshot pin: `B.SNAP_DT = &me_dt.` (hardcoded `'2024-06-30'`, line 61)
- Window: implicit — `TREATMT_STRT_DT` to `TREATMT_END_DT` (campaign-natural end, not fixed +89d)

**VVD SQL approach**
- Tactic filter: `substr(t.TACTIC_ID, 8, 3) = 'VCN'` — all VCN tactics in date range (SQL line 261)
- Success table: `DDWV01.VISA_DR_CRD_DLY` joined on `a.CLNT_NO = card.CLNT_NO`
- Success criteria: `card.STS_CD IN ('06', '08')` AND `card.SRVC_ID = 36` AND **`card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)`** (SQL lines 226–229)
- Snapshot pin: `card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)` — always latest snapshot
- Window: explicit `[STRT, STRT+89]` with 30/60/90d milestones

**Logic divergences**

| # | Dimension | Dashboard SAS | VVD SQL | Materiality |
|---|---|---|---|---|
| D1 | **Success date field** | `B.ACTV_DT` (activation date) | `card.ISS_DT` (issuance date) | **BLOCKING.** VVD SQL says ACTV_DT is wrong for acquisition. Digital cards auto-activate at issuance — using ACTV_DT under-counts non-digital cards and measures a different event entirely. |
| D2 | **Snapshot pin** | `SNAP_DT = '2024-06-30'` (hardcoded single date) | `SNAP_DT = MAX(SNAP_DT)` (always latest) | MATERIAL. Hardcoded date means SAS only sees cards that existed in the snapshot at that specific date. New cards issued after 2024-06-30 are invisible. |
| D3 | **Window boundary** | `TREATMT_END_DT` (campaign-natural) | `TREATMT_STRT_DT + 89` (fixed +89d) | Moderate. Campaign natural end may differ from +89d, especially for quarterly tactics. Affects vintage comparability. |
| D4 | **Tactic scope** | Single tactic literal `'2024001VCN'` | All VCN tactics via SUBSTR filter | MATERIAL. SAS is locked to one deployment; VVD SQL covers full campaign lifecycle. |
| D5 | **`SRVC_ID = 36` filter** | Not present in SAS | Present in VVD SQL (SQL line 227) | Minor. STS_CD '06'/'08' is likely already Visa Debit specific, but the filter adds precision. |
| D6 | **Secondary metric** | None (SAS only produces SUCCESS_IND = 1) | Separate `sec` subquery: first purchase in window using PT_OF_SALE_TXN | Scope difference — SAS is single-indicator only. |

**SAS flags resolvable from VVD SQL**

- FLAG 1 (`TREATMT_MN` field): VVD SQL does not use `TREATMT_MN` at all — it groups by `TACTIC_ID` + `TREATMT_STRT_DT`. Whether the SAS intended `TREATMT_DT_MN` or another variant is UNRESOLVED from VVD SQL's perspective; the field is simply not part of the locked logic.
- FLAG 2 (`ME_DT` date literal quoting): VVD SQL avoids this entirely by using `(SELECT MAX(SNAP_DT) ...)` — the hardcoded date literal approach is eliminated. Resolution: replace `&me_dt.` with dynamic MAX(SNAP_DT) subquery.

---

## Campaign: VDA (Seasonal Acquisition — card acquisition)

**Dashboard SAS approach**
- Tactic filter: `substr(tactic_id, 8, 3) = 'VDA'` (line 40) — same as VVD SQL pattern
- Two parallel success queries:
  - **success01** (`TACTIC_EVNT_IP_AR_HIST` + `VISA_DR_CRD_DLY`): card activation — `B.ACTV_DT BETWEEN TREATMT_STRT_DT AND TREATMT_END_DT`, `STS_CD IN ('06','08')`, `SNAP_DT = &me_dt.` (line 74–75)
  - **success02** (`TACTIC_EVNT_INFO_HIST` + `PT_OF_SALE_TXN`): first purchase — `TXN_DT BETWEEN TREATMT_STRT_DT AND TREATMT_END_DT`, `SRVC_CD=36`, `AMT1>0`, `txn_tp IN ('10','13')`, **`MSG_TP='0210'`** (line 109)
- Window: `TREATMT_STRT_DT` to `TREATMT_END_DT` (campaign-natural)

**VVD SQL approach**
- Tactic filter: `substr(a.TACTIC_ID, 8, 3) = 'VDA'` (SQL line 294) — identical pattern
- **Primary**: `VISA_DR_CRD_DLY`, `ISS_DT BETWEEN STRT AND STRT+89`, `STS_CD IN ('06','08')`, `SRVC_ID=36`, latest snapshot
- **Secondary**: `PT_OF_SALE_TXN`, `MSG_TP='0220'`, `AMT1>0`, `TXN_TP IN ('10','13') AND RCNCL_REAS_CD IN ('M','S')` OR `TXN_TP='22' AND RCNCL_REAS_CD IN ('P','E')` (SQL lines 313–316)
- Window: `[STRT, STRT+89]` with milestones

**Logic divergences**

| # | Dimension | Dashboard SAS | VVD SQL | Materiality |
|---|---|---|---|---|
| D1 | **Primary success field** | `B.ACTV_DT` (activation) | `card.ISS_DT` (issuance) | **BLOCKING.** Same problem as VCN. ACTV_DT is VDT's metric. |
| D2 | **Snapshot pin** | `SNAP_DT = '2024-06-30'` (hardcoded) | `MAX(SNAP_DT)` | MATERIAL. Same limitation as VCN. |
| D3 | **MSG_TP for secondary** | `MSG_TP = '0210'` (auth response, line 109) | `MSG_TP = '0220'` (settlement/financial completion) | **BLOCKING.** Auth responses include failed transactions that never settled. Over-counts success. VVD SQL explicitly calls this out in validation md §5, item 5. |
| D4 | **Secondary reason code filter** | None (SAS has no `RCNCL_REAS_CD`) | `RCNCL_REAS_CD IN ('M','S')` required | MATERIAL. Without reason code filter, reversal transactions and non-standard transaction types contaminate the purchase signal. |
| D5 | **Secondary source table** | `TACTIC_EVNT_INFO_HIST` (line 104) | `TACTIC_EVNT_IP_AR_HIST` (SQL line 308) | Moderate. Different history table — may have different row coverage. VVD SQL consistently uses `IP_AR_HIST`. |
| D6 | **Window boundary** | `TREATMT_END_DT` | `STRT + 89` | Moderate. Same issue as VCN. |
| D7 | **Two-table history split** | success01 uses `IP_AR_HIST`, success02 uses `INFO_HIST` (FLAG 1 in SAS) | Both use `IP_AR_HIST` | Flag resolvable — see below. |

**SAS flags resolvable from VVD SQL**

- FLAG 1 (two history tables `IP_AR_HIST` vs `INFO_HIST`): VVD SQL uses `TACTIC_EVNT_IP_AR_HIST` exclusively for both primary and secondary. The `INFO_HIST` table in success02 is a divergence, not an intentional split. **VVD SQL says: use `IP_AR_HIST` for both.**
- FLAG 2 (`SUCCESS1_IND` naming): Purely cosmetic — VVD SQL uses `SUCCESS_IND` as a column alias concept at the aggregate level. UNRESOLVED as a naming convention question; no impact on logic.
- FLAG 3 (`DDWV01.PT_OF_SALE_TXN` existence): Confirmed — VVD SQL uses `DDWV01.PT_OF_SALE_TXN` at SQL lines 244, 309. The table exists and is correct under `DDWV01`.

---

## Campaign: VUI (Usage Trigger — card usage)

**Dashboard SAS approach**
- Tactic filter: `substr(TACTIC.tactic_id, 8, 3) IN ('VUI')` (line 52, 82)
- Campaign date range: `TREATMT_STRT_DT BETWEEN DATE '2024-08-19' AND DATE '2024-10-17'` (hardcoded macro vars, lines 20–21, 53, 83)
- Success table: `DDWV01.PT_OF_SALE_TXN` joined to `TACTIC_EVNT_INFO_HIST`
- Success criteria (VUI_USAGE block, line 78–84):
  - `TXN_DT BETWEEN TREATMT_STRT_DT AND (TREATMT_STRT_DT + 90)` (hardcoded 90d window)
  - `SRVC_CD IN (36)` — present
  - `/* AND C.AMT1 > 0 */` — **COMMENTED OUT** (line 80 in SAS)
  - `txn_tp IN (10, 13)` — present but as integers, not strings (line 81)
  - No `MSG_TP` filter
  - No `RCNCL_REAS_CD` filter
- Join: `tactic.clnt_no = substr(C.clnt_crd_no, 7, 9)` on `INFO_HIST` (line 76, 84)

**VVD SQL approach**
- Tactic filter: `substr(a.TACTIC_ID, 8, 3) = 'VUI'` — all VUI tactics (SQL line 170)
- Date range: `TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'` (SQL line 171)
- Success table: `DDWV01.PT_OF_SALE_TXN` joined to `TACTIC_EVNT_IP_AR_HIST`
- Success criteria (SQL lines 161–169):
  - `TXN_DT BETWEEN STRT AND (STRT + 89)` (explicit +89)
  - `SRVC_CD = 36`
  - `MSG_TP = '0220'` (settlement only)
  - `AMT1 > 0` (required)
  - `(TXN_TP IN ('10','13') AND RCNCL_REAS_CD IN ('M','S')) OR (TXN_TP='22' AND RCNCL_REAS_CD IN ('P','E'))`
- Join: same `SUBSTR(c.CLNT_CRD_NO, 7, 9)` pattern on `IP_AR_HIST`

**Logic divergences**

| # | Dimension | Dashboard SAS | VVD SQL | Materiality |
|---|---|---|---|---|
| D1 | **AMT1 > 0 filter** | Commented out (line 80) | **Required** (SQL line 164) | **BLOCKING.** Commented-out filter allows zero-value transactions, which includes wallet keep-alive auths and reversals. Inflates success counts. Validation md §7 confirms this is a bug. |
| D2 | **MSG_TP filter** | Not present | `MSG_TP = '0220'` required | **BLOCKING.** Without MSG_TP, auth requests (0210) that never settled are counted as successful purchases. |
| D3 | **RCNCL_REAS_CD** | Not present | `RCNCL_REAS_CD IN ('M','S')` required | BLOCKING. Same consequence as MSG_TP absence — reversal/non-settled transactions contaminate count. |
| D4 | **TXN_TP type** | Integers `(10, 13)` (line 81) | Strings `('10', '13')` | Minor. Teradata will implicitly cast, but the VVD SQL uses string literals as per the EDW dictionary. |
| D5 | **Window** | `STRT + 90` (line 78) | `STRT + 89` (SQL line 169) | Minor. 1-day difference; alignment with 90d window definition. VVD SQL uses STRT+89 = 90 days inclusive. |
| D6 | **Source history table** | `TACTIC_EVNT_INFO_HIST` (line 76) | `TACTIC_EVNT_IP_AR_HIST` (SQL line 159) | Moderate. Consistent with VDA FLAG 1 finding — VVD SQL uses `IP_AR_HIST` exclusively. |
| D7 | **TACTIC segment join** | Also pulls `evnt_sts_reas_reltn_hist` for email engagement (lines 42–50) | Not in success SQL (separate concern) | Scope difference, not a bug. Dashboard adds email action flag; VVD SQL is pure success measurement. |

**SAS flags resolvable from VVD SQL**

- FLAG (line 32, `HRP_GRP_CD` vs `RPT_GRP_CD`): VVD SQL does not use `RPT_GRP_CD` in the success query (groups by `TACTIC_ID`, `TREATMT_STRT_DT`, `TST_GRP_CD` only). The field name typo is in a pass-through SELECT and does not affect success counting. UNRESOLVED for SAS correctness — but immaterial to VVD success logic.
- FLAG (line 35, TRIM/INDEX function structure): VVD SQL does not use any segment derivation from `TACTIC_DECISION_VAR_INFO`. That extraction is not part of the locked success logic. UNRESOLVED — out of scope for VVD SQL.
- FLAG (line 44, second `ON` clause): Should be `AND`. Teradata pass-through may handle this or reject it. VVD SQL avoids the join entirely (uses SUBSTR in the ON clause instead of a secondary ON). UNRESOLVED for SAS syntax correctness.
- FLAG (line 124, `A.TACTIC_ID AND B.TACTIC_ID` missing `=`): **CRITICAL SAS BUG** — the final join in `VUI_CAMPAIGN_DATA` (line 124) is missing the `=` operator: `AND A.TACTIC_ID AND B.TACTIC_ID`. This evaluates as a boolean existence test, not an equality join. In SAS/Teradata pass-through this could result in a cross-join or error. VVD SQL says: this join condition must be `AND A.TACTIC_ID = B.TACTIC_ID`.

---

## Campaign: VUT (Tokenization — wallet provisioning)

**Dashboard SAS approach**
- Tactic filter: `tactic_id = '2024232VUT'` (single hardcoded tactic, line 47)
- Date range: hardcoded `func_strt='2024-07-20'`, `camp_end='2024-09-18'` (lines 28–30)
- Success table: `DDWV05.CLNT_CRD_POS_LOG` + `DL_DECMAN.TOKEN_LIST`
- Success criteria (Successia block, lines 83–91):
  - `TXN_DT BETWEEN date &func_strt. AND date &camp_end.` (absolute dates, not tactic-relative)
  - `AMT1 = 0`
  - `SUBSTR(B.CLNT_CRD_NO, 1, 9) = '45190'` — only 45190 BIN prefix (9 chars)
  - `SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'` — only 45199 prefix
  - `SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'`
  - `POS_ENTR_MODE_CD_NON_EMV = 000` (FLAG-05: unquoted)
  - `SRVC_CD = 36`, `TOKEN_WALLET_IND = 'Y'`
  - `TREATMT_END_DT + 30 AS new_end_dt` — extends window by 30d on back end (line 72)
- Primary indicator: `min_dt BETWEEN TREATMT_STRT_DT AND TREATMT_STRT_DT+30` (Successib, line 102)

**VVD SQL approach**
- Tactic filter: `substr(a.TACTIC_ID, 8, 3) = 'VUT'` — all VUT tactics (SQL line 62)
- Date range: `TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'` (SQL line 63)
- Success criteria (SQL lines 52–61):
  - `TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)` (tactic-relative)
  - `AMT1 = 0`
  - `(VISA_DR_CRD_NO LIKE '45190%' OR VISA_DR_CRD_NO LIKE '45199%')` — both BIN prefixes
  - `POS_ENTR_MODE_CD_NON_EMV = '000'` (quoted string)
  - `APPROVAL_CODE IS NOT NULL` (anti-inflation guard — absent in SAS)
  - `SRVC_CD = 36`, `TOKEN_WALLET_IND = 'Y'`
- Window: `[STRT, STRT+89]` with 30/60/90d milestones
- Granularity: `(CLNT_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID)` — combo-level, then MIN per combo

**Logic divergences**

| # | Dimension | Dashboard SAS | VVD SQL | Materiality |
|---|---|---|---|---|
| D1 | **TXN_DT window reference** | Absolute hardcoded dates `func_strt` to `camp_end` (lines 83, 146) | Tactic-relative `STRT` to `STRT+89` | **BLOCKING.** Hardcoded dates mean the query is frozen to a single campaign run. VVD SQL is tactic-relative and reusable. |
| D2 | **BIN prefix 45190** | `SUBSTR(B.CLNT_CRD_NO, 1, 9) = '45190'` — 9-character match on a 5-character BIN (always false unless field is short) | `VISA_DR_CRD_NO LIKE '45190%'` — correct prefix match | **BLOCKING.** The SAS filter `SUBSTR(B.CLNT_CRD_NO, 1, 9)` on a 5-digit BIN value `'45190'` will never match (comparison of 9 chars vs 5). This could zero out the entire result set. |
| D3 | **BIN prefix 45199** | `SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'` only (lines 86, 149) | `LIKE '45190%' OR LIKE '45199%'` | MATERIAL. SAS only checks 45199 on `VISA_DR_CRD_NO`, missing 45190 on that field. VVD SQL checks both BIN prefixes on `VISA_DR_CRD_NO`. |
| D4 | **`APPROVAL_CODE IS NOT NULL`** | Not present in SAS | Required in VVD SQL (SQL line 57) | **BLOCKING.** This is the primary anti-inflation guard against recurring keep-alive auths. Without it, ~6× inflation per validation md §5 item 4. |
| D5 | **`+30d` window extension** | `TREATMT_END_DT + 30 AS new_end_dt` (line 72), primary checked against `STRT+30` (line 102) | `STRT+89` with 30/60/90d milestones | MATERIAL. SAS adds undocumented 30d back-end buffer beyond `TREATMT_END_DT`. VVD SQL uses explicit `STRT+89`. Validation md §6 item 5: "Not adopted." |
| D6 | **Granularity** | `GROUP BY` on client + tactic fields only; `min_dt` is per-client | `GROUP BY (CLNT_NO, TACTIC_ID, TREATMT_STRT_DT, VISA_DR_CRD_NO, TOKN_REQSTR_ID)` — per card-wallet combo | MATERIAL. SAS counts one MIN per client. VVD SQL counts one MIN per (card, wallet). Secondary metric collapses recurring auths per combo first. |
| D7 | **`TOKN_REQSTR_ID > '0'` filter** | `SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'` (line 87) | Not present (covered by TOKEN_LIST join) | Minor. VVD SQL relies on `TOKEN_LIST` join to ensure valid wallet IDs. |

**SAS flags resolvable from VVD SQL**

- FLAG-04 (catalog `DB6MAPS` vs `DDWV05`): **VVD SQL says `DDWV05.CLNT_CRD_POS_LOG`** (SQL line 51). Use DDWV05.
- FLAG-05 (`POS_ENTR_MODE_CD_NON_EMV = 000` unquoted vs `'000'`): **VVD SQL says `= '000'`** (string literal, SQL line 56). Must be quoted.
- FLAG-06 (`6.rpt_grp_cd` typo): Out of scope for VVD SQL success logic — `rpt_grp_cd` not used in success aggregation. UNRESOLVED for SAS syntax, but immaterial to success counting.
- FLAG-07 (different `CLNT_CRD_NO` SUBSTR positions in Success2a): VVD SQL uses `VISA_DR_CRD_NO LIKE '45190%' OR LIKE '45199%'` — a LIKE pattern, not a SUBSTR position. The position discrepancy in the SAS (`1,9` vs `3,5`) is moot; adopt the LIKE pattern.
- FLAG-08 (`/*38033*/` comment): UNRESOLVED — VVD SQL provides no information on this marker.
- FLAG-09 (`min_min_dt` alias): Minor SAS alias issue; UNRESOLVED — VVD SQL doesn't alias intermediate results this way.
- FLAG-10 (`SUCCESSI_IND` vs `SUCCESS1_IND` name mismatch in COALESCE): UNRESOLVED as a naming convention — VVD SQL uses aggregate column names only.
- FLAG-01 (`dlcreatable` option): UNRESOLVED — SAS options, not SQL logic.
- FLAG-02 (`CLT_NO` vs `CLNT_NO`): **VVD SQL says the field is `CLNT_NO`** (SQL lines 44, 66 etc.). The `CLT_NO` reading is a transcription error.
- FLAG-03 (`Successia` table name): UNRESOLVED — cosmetic naming, no impact.

---

## Campaign: VAW (Add To Wallet — wallet provisioning)

**Dashboard SAS approach**
- Tactic filter: `SUBSTR(TACTIC_ID, 8, 3) = 'VAW'` (line 41) — multi-tactic via SUBSTR
- Success01 block (lines 70–93): wallet provisioning via `DG6V01.CLNT_CRD_POS_LOG` + `DI_DECMAN.TOKEN_LIST`
  - `TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT` (pre-window lookback of 30d, line 78)
  - `AMTS = ?` (FLAG-01: unreadable), `CLNT_CRD_NO` prefix `'45190'` (1,5), `VISA_OR_CRD_NO` prefix `'45190'` (FLAG-02)
  - `SRVC_CD = 38` (FLAG-05: different from 36), `TOKEN_WALLET_IND = 'V'` (FLAG-06: 'V' not 'Y')
- Success02 (final block, lines 152–173): card usage via `DG6V01.PT_OF_SALE_TXN` + `DG6V01.TACTIC_EVNT_INFO_HIST`
  - `TXN_DT BETWEEN TREATMT_STRT_DT AND (TREATMT_STRT_DT + 90)`
  - `SRVC_CD IN (36)`, txn_tp commented out (line 167)
  - `substr(TACTIC.TACTIC_ID, 8, 12) IN ('VAW')` — length 12 (FLAG-03)
- Window: mixed — provisioning uses `[STRT-30, END]`; usage uses `[STRT, STRT+90]`

**VVD SQL approach**
- Tactic filter: `substr(a.TACTIC_ID, 8, 3) = 'VAW'` (SQL line 110)
- Same filter set as VUT (SQL lines 99–113): `DDWV05.CLNT_CRD_POS_LOG`, `DL_DECMAN.TOKEN_LIST`
  - `SRVC_CD = 36`, `AMT1 = 0`, both LIKE `45190%` and `45199%`, `POS_ENTR_MODE_CD_NON_EMV = '000'`, `APPROVAL_CODE IS NOT NULL`, `TOKEN_WALLET_IND = 'Y'`
  - `TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)` (tactic-relative, no pre-window lookback)
- Window: `[STRT, STRT+89]` with 30/60/90d milestones

**Logic divergences**

| # | Dimension | Dashboard SAS | VVD SQL | Materiality |
|---|---|---|---|---|
| D1 | **Catalog for POS_LOG** | `DG6V01.CLNT_CRD_POS_LOG` (line 72) | `DDWV05.CLNT_CRD_POS_LOG` (SQL line 99) | **BLOCKING.** Wrong library. VVD SQL (and vut.sas FLAG-04 resolution) confirm DDWV05. DG6V01 is the tactic history library, not the POS log library. |
| D2 | **Token library** | `DI_DECMAN.TOKEN_LIST` (line 75) | `DL_DECMAN.TOKEN_LIST` (SQL line 107) | **BLOCKING.** `DI_` vs `DL_` prefix — different library. VVD SQL says `DL_DECMAN`. |
| D3 | **`TOKEN_WALLET_IND` value** | `= 'V'` (line 85, FLAG-06) | `= 'Y'` (SQL line 109) | **BLOCKING.** `'V'` will produce zero matches — the lookup table only has `'Y'` as the valid wallet indicator per the VVD SQL and validation md §2. |
| D4 | **SRVC_CD** | `= 38` (line 84, FLAG-05) | `= 36` (SQL line 53) | **BLOCKING.** SRVC_CD 36 = Visa Debit. SRVC_CD 38 is a different service code. Zero-dollar wallet provisioning auths for VVD should be SRVC_CD=36. |
| D5 | **Pre-window lookback** | `TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT` (line 78) | No lookback; `STRT` to `STRT+89` only | MATERIAL. SAS looks 30 days BEFORE campaign start — this would attribute pre-campaign provisioning activity to campaign success. VVD SQL (and validation md §6 item 1) explicitly rejects this. |
| D6 | **`AMTS` filter** | `AMTS = ?` (FLAG-01: value unreadable) | `AMT1 = 0` (SQL line 53) | **BLOCKING.** Field name `AMTS` may be a transcription of `AMT1`. VVD SQL says filter is `AMT1 = 0`. Value `?` is unreadable — likely should be `0`. |
| D7 | **`VISA_OR_CRD_NO` field** | `VISA_OR_CRD_NO` (FLAG-02: uncertain) | `VISA_DR_CRD_NO` (SQL line 55) | MATERIAL. `VISA_OR_CRD_NO` is a suspected typo for `VISA_DR_CRD_NO`. VVD SQL confirms the correct field name. |
| D8 | **`APPROVAL_CODE IS NOT NULL`** | Not present | Required (SQL line 105) | BLOCKING. Same inflation issue as VUT. |
| D9 | **`substr(TACTIC_ID, 8, 12)` in Success02** | `(8, 12)` — 12-char extraction (lines 140, 168, FLAG-03) | `(8, 3)` | **CRITICAL BUG.** `SUBSTR(TACTIC_ID, 8, 12)` extracts 12 characters starting at position 8. For `XXXXXXVVAW`, this returns `VAW` + 9 padding characters. Comparing to `'VAW'` (3 chars) will fail in Teradata since the result is `'VAW         '`. This could silently return zero rows. |
| D10 | **Window for Success02** | `STRT + 90` with `txn_tp` commented out | `STRT + 89` with `txn_tp IN ('10','13')` required | MATERIAL. Same issues as VUI: commented-out filters and 1-day window difference. |
| D11 | **Success02 purpose** | Card usage (second metric) | VVD SQL has no usage secondary for VAW — VAW is provisioning only | Scope difference. VVD SQL treats VAW as provisioning-only. The usage block in the SAS is a diagnostic/secondary indicator not adopted by the VVD locked logic. |

**SAS flags resolvable from VVD SQL**

- FLAG-01 (`AMTS = ?` unreadable): **VVD SQL says `AMT1 = 0`**. The filter is `AMT1 = 0` (zero-dollar provisioning auth).
- FLAG-02 (`VISA_OR_CRD_NO` uncertain): **VVD SQL says `VISA_DR_CRD_NO`**. The correct field name is `VISA_DR_CRD_NO`, matching VVD project memory.
- FLAG-03 (`substr(TACTIC_ID, 8, 12)` should be `8, 3`): **VVD SQL says `substr(a.TACTIC_ID, 8, 3)`**. The `12` is confirmed as an error.
- FLAG-04 (macro var names `&&EHAVE_STRT` / `&&EHAVE_END_DT`): UNRESOLVED from VVD SQL — these are SAS macro variable names. Likely `&&BEHAVE_STRT` / `&&BEHAVE_END` based on VUI usage, but VVD SQL uses date range filters, not these variables.
- FLAG-05 (`SRVC_CD = 38` vs `36`): **VVD SQL says `SRVC_CD = 36`** (Visa Debit). SRVC_CD 38 is incorrect.
- FLAG-06 (`TOKEN_WALLET_IND = 'V'` vs `'Y'`): **VVD SQL says `TOKEN_WALLET_IND = 'Y'`**. The `'V'` is a transcription error.
- FLAG-07 (duplicate `CREATE TABLE Success02`): The second definition (lines 152–173) is the one joined in `tactic_flags` — the first is effectively dead code (diagnostic distribution query). VVD SQL has no equivalent of the first Success02. UNRESOLVED for dashboard SAS intent; the second block is the operative definition.
- FLAG-08 (catalog `DG6V01.CLNT_CRD_POS_LOG`): **VVD SQL says `DDWV05.CLNT_CRD_POS_LOG`**. DG6V01 is the tactic library, not the POS log library.
- FLAG-09 (`DI_DECMAN` vs `DL_DECMAN`): **VVD SQL says `DL_DECMAN.TOKEN_LIST`**. The `DI_` prefix in vaw.sas is an error.
- FLAG-10 (`POS_ENTR_MODE_CD_NON_EMV` — operator and value missing): **VVD SQL says `= '000'`** (SQL line 104).
- FLAG-11 (proc freq `tables` statement has no variables): UNRESOLVED — SAS reporting issue, no impact on success logic.

---

## Net Assessment

### Campaigns that are fundamentally aligned

**None of the 5 campaigns are fully aligned.** All 5 have at least one blocking divergence. However, the severity varies:

### Blocking divergences requiring correction before use

| Campaign | Blockers | Summary |
|---|---|---|
| **VCN** | 2 (D1, D2) | ACTV_DT must become ISS_DT; hardcoded snapshot date must become MAX(SNAP_DT). Logic structure is otherwise sound. |
| **VDA** | 3 (D1, D2, D3) | Same ISS_DT and snapshot fixes as VCN, plus MSG_TP must change from '0210' to '0220' + RCNCL_REAS_CD filter. |
| **VUI** | 3 (D1, D2, D3) | AMT1 filter uncomment; add MSG_TP='0220'; add RCNCL_REAS_CD filter. Also critical SAS syntax bug on line 124 (missing `=` in join). |
| **VUT** | 4 (D1, D2, D3, D4) | Window must be tactic-relative (not absolute dates); BIN filter logic is broken (wrong SUBSTR length); APPROVAL_CODE guard is missing; granularity must move to (card, wallet) combo. |
| **VAW** | 6 (D1, D2, D3, D4, D5, D6) | Multiple catalog/library errors (DG6V01 → DDWV05, DI_DECMAN → DL_DECMAN); TOKEN_WALLET_IND = 'Y' not 'V'; SRVC_CD = 36 not 38; no pre-window lookback; APPROVAL_CODE guard missing. Most corrupt of the 5 files. |

### Issues requiring user decision (not resolvable from VVD SQL alone)

1. **VCN/VDA: Tactic scope.** SAS is campaign-specific (one tactic literal). VVD SQL covers all tactics in a date range. For a live dashboard, the user must decide whether to enumerate all tactic IDs or use the SUBSTR pattern.
2. **VUI: Email engagement join.** SAS pulls `evnt_sts_reas_reltn_hist` for channel/segment enrichment (lines 42–50). VVD SQL does not include this. If the dashboard requires email engagement flags alongside success, this join needs to be preserved — but the success logic itself should not be modified.
3. **VUT: `+30d` back-end buffer on `TREATMT_END_DT`.** Validation md §6 item 5 says "not adopted." User should confirm the +30d was intentional in the SAS or a mistake.
4. **VAW: Success02 (usage) block.** VVD SQL has no usage secondary for VAW. Dashboard SAS treats card usage as a secondary VAW indicator. User must decide whether to retain or drop this block.
5. **VUT FLAG-08 `/*38033*/` comment.** Likely a client count, ticket number, or version marker. Cannot be resolved from VVD SQL.

### Priority order for fixes

1. VAW — most errors, highest risk of returning zero or garbage results (SRVC_CD=38, TOKEN_WALLET_IND='V', wrong library names)
2. VUT — BIN filter D2 likely returns zero rows; absolute date window means it will not generalize
3. VUI — critical join syntax bug (line 124) means the final rollup table is incorrect regardless of success filter correctness
4. VDA — MSG_TP='0210' is confirmed wrong; inflates secondary metric
5. VCN — cleanest of the 5; fix ISS_DT and snapshot pin and it matches VVD SQL structure
