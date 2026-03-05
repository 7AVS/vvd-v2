# VVD v1 Catalog: Data Dictionary

> Generated from the v1 analysis archive. Complete 199-field data dictionary for `final_df`, organized by functional group.

---

## Campaign Identifiers

| Field | Type | Description |
|-------|------|-------------|
| CLNT_NO | string | Client identifier -- primary key for client-level joins |
| TACTIC_ID | string | Unique tactic/deployment identifier |
| MNE | string | Campaign mnemonic (VCN, VDA, VDT, VUI, VAW, VUT) |
| TST_GRP_CD | string | Test group code (TG4=Action, TG7=Control) |
| TREATMT_STRT_DT | date | Treatment start date |
| TREATMT_END_DT | date | Treatment end date |
| measurement_end_dt | date | Measurement window end |
| CAMPAIGN_TYPE | string | Trigger or Batch |
| CAMPAIGN_NAME | string | Full campaign name |
| TACTIC_SEG | string | Tactic segment / targeting approach |
| RPT_GRP_CD | string | Report group code |
| CHANNEL | string | Communication channel |

---

## Card Information

| Field | Type | Description |
|-------|------|-------------|
| Card_Type | string | Card type classification -- WARNING: 92% null/unknown |
| Card_Type_txn | string | Card type for transactions |
| CARD_TYPE_MATCH | integer | Card type match indicator |
| ISS_DT | date | Card issue date |
| ACTV_DT | date | Card activation date |
| DAYS_FROM_ISSUE_TO_ACTIVATION | integer | Days from issue to activation |
| DAYS_FROM_ISSUE_TO_ACTIVATION_v2 | integer | Alternative calculation |

---

## Success Metrics (Binary 0/1)

| Field | Type | Description |
|-------|------|-------------|
| ACQUISITION_SUCCESS | integer | Card acquired (ISS_DT based, STS_CD in [06,08], SRVC_ID=36) |
| ACTIVATION_SUCCESS | integer | Card activated (ACTV_DT based) |
| USAGE_SUCCESS | integer | Card used for POS transaction |
| PROVISIONING_SUCCESS | integer | Card provisioned to digital wallet |
| ISSUE_TO_ACTIVATION_SUCCESS | integer | Issue-to-activation success |

---

## Transaction Metrics

| Field | Type | Description |
|-------|------|-------------|
| TRANSACTION_COUNT | long | Number of POS transactions |
| TOTAL_DOLLAR_AMOUNT | double | Total $ amount |
| ACTIVE_USE_DAYS | long | Days with activity |
| FIRST_TXN_DATE | date | First transaction date |
| LAST_TXN_DATE | date | Last transaction date |
| TXN_DT | date | Transaction date |
| USG_DT | date | Usage date |
| ACTIVATION_COUNT | long | Count of activations |

---

## Customer Demographics (UCP-sourced)

| Field | Type | Description |
|-------|------|-------------|
| AGE | integer | Client age |
| AGE_RNG | integer | Age range code |
| GENERATION | string | Generation classification |
| CLIENT_SEGMENT | string | Client segment |
| INCOME_AFTER_TAX_RNG | integer | Income range code (1-6) |
| TENURE_RBC_YEARS | integer | Years with RBC (fractional) |
| TENURE_RBC_RNG | string | Tenure range bucket |
| CREDIT_SCORE_RNG | integer | Credit score range code (1-5) |
| DLQY_IND | integer | Delinquency indicator |
| PROF_TOT_ANNUAL | decimal(18,4) | Annual profitability |
| PROF_TOT_MONTHLY | float | Monthly profitability |
| PROF_SEG_CD | string | Profitability segment code |
| REL_TP_SEG_CD | string | Relationship type segment |

---

## Digital Engagement

| Field | Type | Description |
|-------|------|-------------|
| OLB_ENROLLED_IND | integer | Online banking enrolled |
| MOBILE_AUTH_CNT | integer | Mobile authentication count |
| MOBILE_AUTH_NOB_CNT | integer | Mobile auth NOB count |
| CPC_OLB_ELIGIBLE | integer | OLB eligible flag |
| CPC_OLB_ENR_RNG | integer | OLB enrollment range |
| CC_MOBILE_ACTIVE | integer | Credit card mobile active |
| PC_MOBILE_ACTIVE | integer | PC mobile active |

---

## Product Holdings -- Credit Cards (all binary 0/1)

| Field | Type | Description |
|-------|------|-------------|
| CC_VISA_ALL_TOT_IND | integer | Any Visa card indicator |
| CC_VISA_DR_IND | integer | Visa debit indicator |
| CC_VISA_CLSIC_TOT_IND | integer | Visa Classic total indicator |
| CC_VISA_CLSIC_RWD_TOT_IND | integer | Visa Classic Rewards total indicator |
| CC_VISA_GOLD_PRFR_TOT_IND | integer | Visa Gold Preferred total indicator |
| CC_VISA_IAV_TOT_IND | integer | Visa IAV total indicator |
| CC_VISA_CLSC_CASH_DOL_IND | integer | Visa Classic Cash Dollar indicator |
| CC_VISA_GOLD_MIN_RWT_IND | integer | Visa Gold Min Rewards indicator |
| CC_VISA_CASH_PLAT_TOT_IND | integer | Visa Cash Platinum total indicator |
| CC_MASTERCARD_ALL_TOT_IND | integer | Mastercard all total indicator |
| MASTERCARD_ALL_TOT_IND | integer | Mastercard all total indicator (duplicate -- see notes) |
| MASTERCARD_CLSC_ALL_TOT_IND | integer | Mastercard Classic all total indicator |
| MASTERCARD_WO_ALL_TOT_IND | integer | Mastercard World all total indicator |

---

## Product Holdings -- Loans & Services

| Field | Type | Description |
|-------|------|-------------|
| LOAN_TOT_IND | integer | Loan indicator |
| MORTGAGE_RESID_TOT_IND | integer | Residential mortgage |
| LOAN_RCL_UNSEC_TOT_IND | integer | Unsecured revolving loan |
| MULTI_PROD_RBT_TOT_IND | integer | Multi-product rebate |
| ANY_GC_TOT_IND | integer | Any GC product |
| ANY_STUDENT_TOT_IND | integer | Any student product |
| ANY_PREMIUM_TOT_IND | integer | Any premium product |

---

## Services & Activity Counters

| Field | Type | Description |
|-------|------|-------------|
| SRVC_CNT | integer | Services count |
| T_TOT_CNT | integer | T product category count |
| I_TOT_CNT | integer | I product category count |
| B_TOT_CNT | integer | B product category count |
| C_TOT_CNT | integer | C product category count |
| MULTI_PEG | integer | Multi-PEG flag |
| PROG_ENRL_TAG | integer | Program enrollment tag |

---

## Channel Preferences

| Field | Type | Description |
|-------|------|-------------|
| D2D_BILL_PYMT_CHNL_PREF_SEG_CD | string | Bill payment channel preference segment |
| D2D_DEP_CHNL_PREF_SEG_CD | string | Deposit channel preference segment |
| D2D_INFO_SEEK_CHNL_PREF_SEG_CD | string | Info seeking channel preference segment |
| D2D_TRF_CHNL_PREF_SEG_CD | string | Transfer channel preference segment |
| D2D_WD_CHNL_PREF_SEG_CD | string | Withdrawal channel preference segment |

---

## Household & Family

| Field | Type | Description |
|-------|------|-------------|
| DTC_FAMILY_SIZE | float | DTC family size |
| ANT_DTC_FAMILY_SIZE | float | ANT DTC family size |
| FAMILY_SIZE | float | Family size |
| MULTI_FAM_CNT | integer | Multi-family count |
| MB_MULTI_FAM_CNT | integer | MB multi-family count |
| HH_MNGY_TAG | integer | Household money tag |

---

## Segmentation & Lifecycle

| Field | Type | Description |
|-------|------|-------------|
| ANT_CSTM_TOT_SEG | string | ANT custom total segment |
| NEW_CSTM_TOT_SEG_01 | string | New custom total segment 01 |
| NEW_CSTM_TOT_SEG_02 | string | New custom total segment 02 |
| ANT_ACQU_BEHAV_SEG_CD | string | ANT acquisition behavior segment code |
| ANT_SEG_CD | string | ANT segment code |
| GENR_STRATA | string | Generation strata |
| ANT_CSTM_LIFE_STAGE | string | ANT custom life stage |
| LIFE_STAGE_GRPS | integer | Life stage groups |

---

## Geographic

| Field | Type | Description |
|-------|------|-------------|
| MB_PROV_ST_CD | string | Province/state code |
| MB_CITY_CD | string | City code |
| MB_COMMUNITY_SIZE | string | Community size |
| FSA | string | Forward sortation area |

---

## Additional Demographics

| Field | Type | Description |
|-------|------|-------------|
| MB_ACCT_AGE | integer | Account age |
| MB_AGE | integer | Member age |
| MB_LANG_CD | string | Language code |
| MB_ENRL_LANG | string | Enrollment language |
| MB_GENDER | string | Gender |
| MB_CLN_TP | integer/string | Client type |
| MB_CLN_CLNT | string | Client classification |

---

## Spend & Profit Categories

| Field | Type | Description |
|-------|------|-------------|
| MB_CC_CLN_SPEND_CAT_SEG_CD | integer | CC spend category segment code |
| MB_CC_CLN_PRFT_SEG_CD | integer | CC profit segment code |
| MB2_SPEND_CAT_SEG_CD | integer | MB2 spend category segment code |
| MB2_3_SPEND_CAT_SEG_CD | integer | MB2/3 spend category segment code |
| MB2_CC_CLN_PRFT_SEG_CD | integer | MB2 CC profit segment code |
| MB2_3_CLN_PRFT_SEG_CD | integer | MB2/3 profit segment code |

---

## Activation & Usage Flags

| Field | Type | Description |
|-------|------|-------------|
| CC_FRG_FL_ACTIVATION | integer | CC foreign flag activation |
| CC_OSB_FL_ACTIVATION | integer | CC OSB flag activation |
| CC_OLW_FL_ACTIVATION | integer | CC OLW flag activation |
| RC_OLW_ENRL | integer | RC OLW enrollment |
| NEW_ACTIVATION | integer | New activation flag |

---

## Journey & Lifecycle Flags

| Field | Type | Description |
|-------|------|-------------|
| FLG_MHRT_RWD | integer | MHRT rewards flag |
| FLG_CT_to_RTAP | integer | CT to RTAP transition flag |
| FLG_CT_to_MHRT | integer | CT to MHRT transition flag |
| FLG_CT_TO_CT | integer | CT to CT transition flag |
| FLG_ALL_TO_ALL | integer | All to all transition flag |
| FLG_MHRT_REC_CD | integer | MHRT recommendation code flag |
| CC_FULL_LIFECYCLE | integer | Full lifecycle indicator |
| CC_NON_LIFECYCLE | integer | Non-lifecycle indicator |

---

## Timing Metrics

| Field | Type | Description |
|-------|------|-------------|
| FRST_MSG_DT | date | First message date |
| DTE_LAST_ACTIVATION | integer | Days to last activation |
| TOT_DAYS_AFTER | long | Total days after |

---

## Qualification Metrics

| Field | Type | Description |
|-------|------|-------------|
| ANT_ACTVEL | integer | ANT active flag |
| ANT_USAGE | integer | ANT usage flag |
| ANT_REWARDS | integer | ANT rewards flag |
| ANT_PREMIUM | integer | ANT premium flag |
| ANT_STUDENT | integer | ANT student flag |
| ANT_NO_FEE | integer | ANT no-fee flag |
| QC_QUAL | integer | QC qualification flag |

---

## UCP Metadata

| Field | Type | Description |
|-------|------|-------------|
| UCP_REFERENCE_DATE | date | UCP snapshot reference date |
| UCP_MONTH_END_DATE | date | UCP month-end date |

---

## Salted/Privacy Fields

| Field | Type | Description |
|-------|------|-------------|
| CLNT_NO_salt | string | Salted client number |
| TACTIC_ID_salt | string | Salted tactic ID |
| ACCT_NUM_salt | string | Salted account number |
| ACTV_DT_SALT | string | Salted activation date |
| CLNT_DT_SALT | string | Salted client date |
| ISSUED_DT_SALT | string | Salted issued date |
| ACTIVATION_DATE_SALT | string | Salted activation date |

---

## Additional Campaign Fields

| Field | Type | Description |
|-------|------|-------------|
| CAMPAIGN_ID | string | Campaign identifier |
| CMPGN_ID | string | Campaign identifier (alternate) |
| CAMPAIGN_NAME_ORIG | string | Original campaign name |
| TACTIC_TYPE | string | Tactic type classification |
| SEGMENTATION_DESC | string | Segmentation description |
| MBG_SCALE_ID | string | MBG scale identifier |
| CMPGN_TARGET | string | Campaign target |
| CMPGN_CNT | integer | Campaign count |

---

## Additional Indicators

| Field | Type | Description |
|-------|------|-------------|
| CC_ION_* series | integer | Multiple ION variant indicators |
| CC_AL_* series | integer | Multiple AL variant indicators |
| CC_BA_PLAT_ALL_TOT_IND | integer | BA Platinum all total indicator |
| EMERALD_IND | integer | Emerald indicator |
| CLNT_SPCFC_CD_VAL | string | Client-specific code value |

---

## Important Notes

1. **Card_Type has mixed case** (not CARD_TYPE) -- 92% null/unknown values.
2. **All fields nullable = true** across the entire schema.
3. **Two similar Mastercard fields**: `CC_MASTERCARD_ALL_TOT_IND` and `MASTERCARD_ALL_TOT_IND` both exist with unclear distinction.
4. **Two versions of issue-to-activation**: `DAYS_FROM_ISSUE_TO_ACTIVATION` and `DAYS_FROM_ISSUE_TO_ACTIVATION_v2` -- unclear which is authoritative.
5. **UCP fields are static monthly snapshots** repeated per deployment row. For client-level analysis, deduplicate with `.select("CLNT_NO").distinct()`.
