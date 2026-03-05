# VVD v1 Catalog: Success Definitions

> Generated from the v1 analysis archive. This document defines the per-campaign success metric mapping, underlying data sources, and v2 resolution decisions.

---

## Campaign to Success Metric Mapping

| Campaign | MNE | Success Column in final_df | Goal | Test/Control Split | Measurement Window |
|----------|-----|---------------------------|------|-------------------|-------------------|
| VVD Contextual Notification | VCN | ACQUISITION_SUCCESS | Acquisition | 95/5 | 30 days (trigger) |
| VVD Black Friday Targeted | VDA | ACTIVATION_SUCCESS* | Acquisition | 95/5 | 90 days (batch) |
| VVD Activation Trigger | VDT | ACTIVATION_SUCCESS | Activation | 90/10 | 30 days (trigger) |
| VVD Usage Trigger | VUI | USAGE_SUCCESS | Usage | 95/5 | 30 days (trigger) |
| VVD Tokenization Campaign | VUT | PROVISIONING_SUCCESS | Provisioning | 95/5 | 30 days (batch) |
| VVD Add To Wallet | VAW | PROVISIONING_SUCCESS | Provisioning | 80/20 | 30 days (trigger) |

*VDA uses ACTIVATION_SUCCESS due to backend data mapping issue -- should conceptually be ACQUISITION_SUCCESS.

---

## Success Metric Definitions (from Vintage/Vvd Pipeline)

### card_acquisition
- **Source**: HIVE `DDWTA_VISA_DR_CRD`
- **Key field**: `ISS_DT` (issue date)
- **Filters**: `STS_CD` in [06, 08], `SRVC_ID = 36`

### card_activation
- **Source**: HIVE `DDWTA_VISA_DR_CRD`
- **Key field**: `ACTV_DT` (activation date)
- **Filters**: `STS_CD` in [06, 08], `SRVC_ID = 36`

### card_usage
- **Source**: HIVE `DDWTA_T_PT_OF_SALE_TXN`
- **Key field**: `TXN_DT` (transaction date)
- **Filters**: `SRVC_CD = 36`, specific TXN_TYPES: [(10, '0210'), (13, '0210'), (12, '0220')], `AMT1 > 0`

### wallet_provisioning
- **Source**: EDW `CLNT_CRD_POS_LOG` joined with `TOKEN_LIST`
- **Key field**: Zero-amount transaction (`AMT1 = 0`)
- **Filters**: BIN in [45190, 45199]

---

## V2 Resolved Decisions

### 1. VDA: Use card_acquisition from Vintage/Vvd
- **Problem**: VDA maps to ACTIVATION_SUCCESS in final_df, but its goal is acquisition.
- **Resolution**: V2 uses `card_acquisition` (ISS_DT based) from the Vintage/Vvd pipeline. This replaces the ACTIVATION_SUCCESS workaround.

### 2. VAW: Use wallet_provisioning from Vintage/Vvd
- **Problem**: VAW uses PROVISIONING_SUCCESS which is available but sourced differently.
- **Resolution**: V2 uses `wallet_provisioning` from the Vintage/Vvd pipeline. This is semantically correct for the "Add To Wallet" campaign goal.
