# VVD v2 Pipeline: UCP Gap Analysis

## The Problem

The VVD v1 analysis used ~100 demographic/behavioral fields from the Unified Client Profile (UCP). These were pre-joined into `final_df.parquet` by someone upstream. **No extraction pipeline exists in either VVD or Vintage/Vvd projects.**

## UCP-Origin Fields in final_df (Complete List)

### Core Demographics (~15 fields)
- AGE, AGE_RNG, GENERATION
- INCOME_AFTER_TAX_RNG
- TENURE_RBC_YEARS, TENURE_RBC_RNG
- CLIENT_SEGMENT
- CREDIT_SCORE_RNG
- DLQY_IND (delinquency)
- PROF_TOT_ANNUAL, PROF_TOT_MONTHLY, PROF_SEG_CD
- REL_TP_SEG_CD
- MB_GENDER, MB_LANG_CD, MB_ENRL_LANG
- MB_AGE, MB_ACCT_AGE

### Digital Engagement (~7 fields)
- OLB_ENROLLED_IND
- MOBILE_AUTH_CNT, MOBILE_AUTH_NOB_CNT
- CPC_OLB_ELIGIBLE, CPC_OLB_ENR_RNG
- CC_MOBILE_ACTIVE, PC_MOBILE_ACTIVE

### Product Holdings — Credit Cards (~13 fields)
- CC_VISA_ALL_TOT_IND, CC_VISA_DR_IND, CC_VISA_CLSIC_TOT_IND
- CC_VISA_CLSIC_RWD_TOT_IND, CC_VISA_GOLD_PRFR_TOT_IND
- CC_VISA_IAV_TOT_IND, CC_VISA_CLSC_CASH_DOL_IND
- CC_VISA_GOLD_MIN_RWT_IND, CC_VISA_CASH_PLAT_TOT_IND
- CC_MASTERCARD_ALL_TOT_IND, MASTERCARD_ALL_TOT_IND
- MASTERCARD_CLSC_ALL_TOT_IND, MASTERCARD_WO_ALL_TOT_IND

### Product Holdings — Loans & Services (~7 fields)
- LOAN_TOT_IND, MORTGAGE_RESID_TOT_IND, LOAN_RCL_UNSEC_TOT_IND
- MULTI_PROD_RBT_TOT_IND
- ANY_GC_TOT_IND, ANY_STUDENT_TOT_IND, ANY_PREMIUM_TOT_IND

### Services & Activity (~6 fields)
- SRVC_CNT, T_TOT_CNT, I_TOT_CNT, B_TOT_CNT, C_TOT_CNT
- MULTI_PEG, PROG_ENRL_TAG

### Channel Preferences (~5 fields)
- D2D_BILL_PYMT_CHNL_PREF_SEG_CD
- D2D_DEP_CHNL_PREF_SEG_CD
- D2D_INFO_SEEK_CHNL_PREF_SEG_CD
- D2D_TRF_CHNL_PREF_SEG_CD
- D2D_WD_CHNL_PREF_SEG_CD

### Household & Family (~6 fields)
- DTC_FAMILY_SIZE, ANT_DTC_FAMILY_SIZE, FAMILY_SIZE
- MULTI_FAM_CNT, MB_MULTI_FAM_CNT
- HH_MNGY_TAG

### Segmentation & Lifecycle (~8 fields)
- ANT_CSTM_TOT_SEG, NEW_CSTM_TOT_SEG_01, NEW_CSTM_TOT_SEG_02
- ANT_ACQU_BEHAV_SEG_CD, ANT_SEG_CD, GENR_STRATA
- ANT_CSTM_LIFE_STAGE, LIFE_STAGE_GRPS

### Geographic (~4 fields)
- MB_PROV_ST_CD, MB_CITY_CD, MB_COMMUNITY_SIZE, FSA

### Spend & Profit Categories (~6 fields)
- MB_CC_CLN_SPEND_CAT_SEG_CD, MB_CC_CLN_PRFT_SEG_CD
- MB2_SPEND_CAT_SEG_CD, MB2_3_SPEND_CAT_SEG_CD
- MB2_CC_CLN_PRFT_SEG_CD, MB2_3_CLN_PRFT_SEG_CD

### Activation & Usage Flags (~5 fields)
- CC_FRG_FL_ACTIVATION, CC_OSB_FL_ACTIVATION, CC_OLW_FL_ACTIVATION
- RC_OLW_ENRL, NEW_ACTIVATION

### Qualification Metrics (~7 fields)
- ANT_ACTVEL, ANT_USAGE, ANT_REWARDS, ANT_PREMIUM, ANT_STUDENT, ANT_NO_FEE
- QC_QUAL

### Journey & Lifecycle Flags (~8 fields)
- FLG_MHRT_RWD, FLG_CT_to_RTAP, FLG_CT_to_MHRT, FLG_CT_TO_CT, FLG_ALL_TO_ALL
- FLG_MHRT_REC_CD
- CC_FULL_LIFECYCLE, CC_NON_LIFECYCLE

### Additional Indicators (~10+ fields)
- CC_ION_* series, CC_AL_* series, CC_BA_PLAT_ALL_TOT_IND
- EMERALD_IND / CLNT_SPCFC_CD_VAL
- MB_CLN_TP, MB_CLN_CLNT

## UCP Metadata Fields
- **UCP_REFERENCE_DATE** (date): Reference date for the UCP snapshot
- **UCP_MONTH_END_DATE** (date): Month-end date of the UCP data

These confirm the data is curated monthly snapshots, not real-time.

## What's Known
1. Data is pre-joined at the client level (CLNT_NO is the join key)
2. Snapshots are monthly (UCP_MONTH_END_DATE confirms this)
3. Fields are static within each monthly snapshot — repeated per deployment row
4. Someone upstream (likely data engineering or the original analyst) performed the join
5. The join was CLNT_NO + snapshot date alignment

## What's Missing
1. **UCP source table name** — Unknown. Likely an internal data warehouse table.
2. **Join logic** — How was the snapshot date aligned with treatment dates?
3. **Refresh cadence** — Monthly? When does it update?
4. **Data lineage** — Who built the original final_df join?
5. **Field definitions** — What exactly does each UCP field measure? (e.g., is INCOME_AFTER_TAX_RNG annual or monthly?)

## Impact on VVD v2

### What We CAN Do Without UCP
- All campaign measurement (success rates, lifts, significance)
- Vintage curve generation
- Channel engagement analysis (email open/click/unsub)
- Overcontacting and orchestration analysis
- Campaign overlap detection

### What We CANNOT Do Without UCP
- Demographic segmentation (age, income, tenure breakdowns)
- Predictive modeling for targeting
- Client profiling (responder vs non-responder characteristics)
- Geographic analysis
- Product affinity analysis
- Profitability-adjusted ROI calculations

## Recommendation

**Status**: BLOCKED on data engineering team input.

**Action Items**:
1. Request UCP source table name and location from data engineering
2. Confirm join key (CLNT_NO + snapshot alignment logic)
3. Get field-level documentation for all ~100 UCP fields
4. Determine refresh cadence and latest available snapshot
5. Build UCP extraction module (M4) once source is identified

**Workaround for Monday Meeting**: Use findings from v1 analysis (which had UCP pre-joined) for demographic insights. Flag as "based on historical data, pending refresh."
