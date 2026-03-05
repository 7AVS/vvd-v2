# VVD v1 Catalog: Known Issues

> Generated from the v1 analysis archive. This document catalogs all known data quality issues and analytical caveats identified during the v1 analysis.

---

## Data Quality Issues

### 1. Card_Type 92% Null
The `Card_Type` field is 92% unknown/null. Cannot reliably segment by card type. Any analysis using this field will be based on only ~8% of the population and should not be considered representative.

### 2. VCN SRM Warnings
Persistent p=0.0000 SRM (Sample Ratio Mismatch) flags across all VCN quarters, suggesting possible sample allocation bias. All VCN results carry this caveat. The expected 95/5 split may not hold in practice.

### 3. VDA Backend Workaround
VDA campaign maps to `ACTIVATION_SUCCESS` instead of `ACQUISITION_SUCCESS` due to a backend data issue. The campaign goal is acquisition, but the success metric used in v1 is activation. Resolved in v2 by using `card_acquisition` from the Vintage/Vvd pipeline.

### 4. UCP Static Snapshots
UCP demographic fields are monthly snapshots repeated per deployment row. This means the same client will have identical demographic values across all their deployments within a month. For client-level analysis, deduplicate with `.select("CLNT_NO").distinct()` to avoid inflating counts.

### 5. Channel Data Missing
98.7% of VCN records are marked "Unknown" for bill payment channel preference (`D2D_BILL_PYMT_CHNL_PREF_SEG_CD`). Cannot optimize channels for VCN based on this data.

### 6. Measurement Windows Not Explicit
`final_df` does not explicitly encode measurement windows. They are implicit in the range from `TREATMT_STRT_DT` to `measurement_end_dt`. Analysts must compute the window manually and be aware that trigger campaigns use 30 days while batch campaigns (VDA, VUT) use 90 days.

### 7. CLNT_NO Leading Zeros
Silent join failures occur if `CLNT_NO` has leading zeros in one dataset but not the other. All modules must strip leading zeros with `REGEXP_REPLACE("^0+", "")` before any join operation.

### 8. Two MASTERCARD Fields
Both `CC_MASTERCARD_ALL_TOT_IND` and `MASTERCARD_ALL_TOT_IND` exist in the schema. The distinction between these two fields is unclear. Exercise caution when using either.

### 9. Two DAYS_FROM_ISSUE_TO_ACTIVATION Versions
Both `DAYS_FROM_ISSUE_TO_ACTIVATION` and `DAYS_FROM_ISSUE_TO_ACTIVATION_v2` exist. It is unclear which version is authoritative or how the calculation differs.

### 10. VUT Negative Revenue
VUT campaign shows -$12K revenue impact. This campaign produces negative ROMI (-18X) and should be considered for discontinuation.

---

## Analytical Caveats

### 1. 17-Month Window Only
Analysis covers January 2024 through May 2025. Cannot observe pre-period activations, long-term behavioral trends, or seasonal patterns beyond this window.

### 2. No Transaction-Level Detail
Only aggregated counts and amounts are available (TRANSACTION_COUNT, TOTAL_DOLLAR_AMOUNT). Individual transactions are not accessible, limiting basket analysis, merchant-level insights, and spending pattern work.

### 3. No Geographic Transaction Data
Cannot determine domestic vs. international usage patterns. Geographic fields (MB_PROV_ST_CD, FSA) reflect client home location, not transaction location.

### 4. VCN Profitability Paradox
VCN responders average $663 annual profitability vs. non-responders at $996. Clients who respond to VCN contextual notifications are systematically less profitable than those who do not. This may indicate the campaign is reaching clients who are more price-sensitive or have lower product engagement.

### 5. Audience Fatigue in VDA
VDA performance decays across treatment periods:
- Period 1: +0.43% lift
- Period 2: +0.18% lift
- Period 3: +0.02% lift

This suggests diminishing returns from repeated batch targeting of the same audience.

### 6. High Baseline Ceiling Effects
VUI has a 29% baseline success rate and VUT has a 10% baseline. High baselines limit the potential for campaign-driven incremental lift, as a large portion of the audience would succeed regardless of treatment.
