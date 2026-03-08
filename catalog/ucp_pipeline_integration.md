# UCP Enrichment — Pipeline Integration Plan

## Where It Fits

**New Cell 4c: UCP Enrichment** — after Cell 4b (Email), before Cell 5 (Analysis).

```
Cell 1:  Config
Cell 2:  M1 — tactic_df (experiment population, ~20M rows)
Cell 3:  M3 — success outcome DataFrames
Cell 4:  M6 — result_df (tactic_df + success flags, persisted)
Cell 4b: Email engagement (optional, re-persists result_df with email columns)
Cell 4c: UCP ENRICHMENT  <-- NEW
Cell 5+: Analysis (reads persisted result_df)
```

The UCP join runs against the already-persisted `result_df`. It adds 52 demographic/product/digital columns, re-persists, and all downstream analysis cells (5-9) can segment by any UCP dimension without pipeline changes.

## Input

**`result_df`** after Cell 4 (or 4b if email ran). Schema at that point:

| Source | Columns |
|--------|---------|
| tactic_df (Cell 2) | CLNT_NO, TACTIC_ID, MNE, TST_GRP_CD, RPT_GRP_CD, TREATMT_STRT_DT, TREATMT_END_DT, TREATMT_MN, TACTIC_CELL_CD, WINDOW_DAYS, COHORT |
| M6 success (Cell 4) | ACQUISITION_SUCCESS, FIRST_ACQUISITION_SUCCESS_DT, ACTIVATION_SUCCESS, FIRST_ACTIVATION_SUCCESS_DT, USAGE_SUCCESS, FIRST_USAGE_SUCCESS_DT, PROVISIONING_SUCCESS, FIRST_PROVISIONING_SUCCESS_DT, SUCCESS |
| Email (Cell 4b) | EMAIL_SENT, EMAIL_OPENED, EMAIL_CLICKED, EMAIL_UNSUBSCRIBED + date columns |

Key numbers: ~20M rows, ~3.4M unique clients (Run 3).

## Join Logic

1. **Derive UCP_MONTH_END_DATE** from `TREATMT_STRT_DT` — maps each row to the month-end partition of its treatment start (e.g., 2025-03-15 maps to 2025-03-31).
2. **Cap at 2025-05-31** — UCP data stops at May 2025. Any `TREATMT_STRT_DT` after May 2025 gets the May 2025 snapshot.
3. **Per-partition extraction** — for each distinct `UCP_MONTH_END_DATE`, read the UCP parquet partition at `/prod/sz/tsz/00172/data/ucp4/MONTH_END_DATE={date}`, select only the 53 needed columns, broadcast-join against the client list for that partition.
4. **Union all partitions** into `ucp_all`.
5. **Left join** `result_df` to `ucp_all` on `CLNT_NO`. Left join preserves all experiment rows even if UCP data is missing.
6. **Re-persist** `result_df` with UCP columns.

### Join Key
- `CLNT_NO` (string) in both datasets. No type conversion needed — both are already stripped-leading-zeros strings at this point in the pipeline.

### UCP_REFERENCE_DATE Decision
The original code uses a `UCP_REFERENCE_DATE` column on `client_base_final` whose derivation is unknown. For VVD v2, the natural choice is **`TREATMT_STRT_DT`** — this gives each client the UCP profile as of when they entered the experiment, which is the correct causal baseline (who they were when treated, not who they became after).

## Columns Added (52 columns, 10 categories)

| Category | Count | Key Columns |
|----------|-------|-------------|
| Demographics | 8 | AGE, AGE_RNG, GENERATION, NXT_GEN_STUD_SEG_CD, NEW_IMGRNT_SEG_CD, LOG_COMP_SEG_CD, ACTV_PROD_CNT, ACTV_PROD_SRVC_CNT, CLNT_SPCFC_CD_VAL |
| Tenure | 2 | TENURE_RBC_YEARS, TENURE_RBC_RNG |
| Credit | 2 | CREDIT_SCORE_RNG, DLQY_IND |
| Income/Value | 4 | INCOME_AFTER_TAX_RNG, PROF_TOT_ANNUAL, PROF_TOT_MONTHLY, PROF_SEG_CD |
| Relationship | 3 | ACCT_MGD_RELTN_TP_CD, REL_TP_SEG_CD, RELN_MG_UNIT_NO |
| Credit Cards | 10 | CC_VISA_ALL_TOT_IND, CC_VISA_DR_IND, CC_VISA_INF_TOT_IND, CC_MASTERCARD_ALL_TOT_IND, etc. |
| Lending | 7 | LOAN_TOT_IND, MORTGAGE_RESID_TOT_IND, LOAN_RCL_UNSEC_TOT_IND, student loan variants |
| Multi-Product | 2 | MULTI_PROD_RBT_TOT_IND, SRVC_CNT |
| Digital/Online | 4 | MOBILE_AUTH_MOB_CNT, OLB_ENROLLED_IND, CPC_OLB_ELIGIBLE, MOBILE_AUTH_CNT |
| Channel Preference | 5 | D2D_BILL_PYMT_CHNL_PREF_SEG_CD, D2D_DEP_CHNL_PREF_SEG_CD, D2D_INFO_SEEK_CHNL_PREF_SEG_CD, D2D_TRF_CHNL_PREF_SEG_CD, D2D_MQ_CHNL_PREF_SEG_CD |
| Transactions | 4 | T_TOT_CNT, I_TOT_CNT, B_TOT_CNT, C_TOT_CNT |
| OFI | 5 | OFI_M_PROD_CNT, OFI_L_PROD_CNT, OFI_C_PROD_CNT, OFI_I_PROD_CNT, OFI_T_PROD_CNT |

Full column list in `catalog/ucp_enrichment_logic.md`.

## What It Enables

### Segmented Lift Analysis (the big unlock)
Currently, Cells 5-9 compute lift at campaign x cohort level only. With UCP:

- **Lift by generation**: Does VCN work better for Millennials vs Boomers?
- **Lift by income tier**: Are high-income clients already acquiring organically (diluting lift)?
- **Lift by credit score**: Do credit-constrained clients respond differently?
- **Lift by tenure**: New-to-bank vs established clients — different response curves?
- **Lift by digital engagement**: Mobile-active clients vs branch-only — does channel alignment matter?

### Targeting Effectiveness
- **VCN's 4,403 deployments/incremental client problem**: Is VCN over-serving clients who would acquire anyway? UCP segments can identify which sub-populations actually move the needle.
- **VUT vs VAW divergence**: VAW works, VUT doesn't. Is the difference explained by client profile (digitally savvy iOS users in VAW vs broader population in VUT)?

### NIBT Refinement
- Current NIBT uses flat $78.21/incremental client. With PROF_TOT_ANNUAL from UCP, we can compute actual profitability of incremental clients vs average — are campaigns acquiring high-value or low-value clients?

### Product Cross-Sell Insights
- 10 credit card indicators + 7 lending indicators = full product holding picture at treatment time. Can identify: do VVD campaigns work better for clients who already have credit cards? Does having a mortgage correlate with VVD adoption?

### OFI Competition Signal
- 5 OFI (Other Financial Institution) columns reveal multi-banking behavior. Clients with high OFI counts may be harder to move — or more valuable if acquired.

## Gotchas

1. **~1 month data lag.** UCP partitions arrive with approximately 1 month delay (e.g., Feb 2026 data may not be available on March 8th). No permanent cap — just update the latest partition date at run time. For months beyond the latest available partition, clients get the most recent snapshot available.

2. **MONTH_END_DATE is a partition, not a column.** You cannot `SELECT MONTH_END_DATE` from the table. You read it via the directory path. The code handles this correctly by constructing partition paths.

3. **~30 partitions to read.** Each is a separate parquet read + broadcast join. Total runtime scales linearly. The original code showed ~100K clients per partition taking seconds — 30 partitions is manageable but not instant.

4. **~1.2% client miss rate.** The original code found 99,268 out of 100,454 clients in the 2024-11-30 partition. Some experiment clients simply don't exist in UCP (new clients, data lags). The left join handles this — missing UCP clients get NULLs. But analysis cells need to handle NULLs in UCP columns.

5. **Duplicate risk on union.** If a client appears in multiple partitions (different treatment dates), they get multiple UCP rows. The join to result_df is on CLNT_NO only — if UCP has duplicates, the join fans out. Need dedup on `ucp_all` by CLNT_NO before the final join, or match on CLNT_NO + UCP_MONTH_END_DATE.

6. **Row fan-out from result_df granularity.** result_df has ~20M rows but only ~3.4M unique clients. A client can appear in multiple campaigns/cohorts. The UCP join should be 1:many (one UCP profile per client, many result_df rows per client) — but if UCP also has duplicates, it becomes many:many. The original code checks for this but doesn't prevent it.

7. **UCP_REFERENCE_DATE must be decided.** The original code used a pre-existing `UCP_REFERENCE_DATE` column on `client_base_final`. For VVD v2, the cleanest mapping is `TREATMT_STRT_DT` (treatment start). But since a client can have multiple treatment starts across campaigns, the UCP partition lookup should be per-row, not per-client. This is what the original code does — it groups clients by their derived UCP_MONTH_END_DATE, not by CLNT_NO.

8. **Blocked on data engineering.** The pipeline header (line 12) says "No UCP enrichment -- demographics excluded (blocked on data engineering)". This suggests UCP access or partition availability may have dependencies outside our control.

## Implementation Steps (When Unblocked)

1. Add `Cell 4c` after Cell 4b in both `vvd_v2_pipeline.py` and `.ipynb`
2. Derive `UCP_MONTH_END_DATE` from `TREATMT_STRT_DT` on `result_df`
3. Use the latest available partition at run time (~1 month lag from current date). No permanent cap — just update the date.
4. Loop per partition: read UCP parquet, select 53 columns, broadcast-join with partition's client list
5. Union all, dedup by (CLNT_NO, UCP_MONTH_END_DATE) — NOT just CLNT_NO. A client in multiple campaigns gets different UCP snapshots per deployment.
6. Derive UCP_MONTH_END_DATE on result_df (same logic as step 2). Left join to result_df on BOTH (CLNT_NO, UCP_MONTH_END_DATE).
7. Re-persist result_df
8. Add UCP-segmented analysis in Cell 5+ (groupBy UCP dimension + MNE + TST_GRP_CD)
9. Validate: check NULL rate per UCP column, verify no row fan-out (count before/after join should match)
