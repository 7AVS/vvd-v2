# VVD v2 Pipeline: Success Library

Imported from Vintage/Vvd project (src/config.py and src/vintage_engine.py v2.5).

## Success Definitions

### 1. card_acquisition
- **Source**: HIVE (Spark parquet)
- **Table**: `/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT=`
- **Date Field**: ISS_DT (Issue Date)
- **Client Key**: CLNT_NO
- **Filters**:
  - STS_CD IN ['06', '08'] (Active/Approved status)
  - SRVC_ID = 36 (Visa Direct service)
  - ISS_DT IS NOT NULL
- **Business Rule**: Client acquired a new VVD card in active/approved status
- **Used By**: VCN (primary), VDA (primary)

### 2. card_activation
- **Source**: HIVE (Spark parquet)
- **Table**: `/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT=`
- **Date Field**: ACTV_DT (Activation Date)
- **Client Key**: CLNT_NO
- **Filters**:
  - STS_CD IN ['06', '08'] (Active/Approved)
  - SRVC_ID = 36 (Visa Direct)
  - ISS_DT IS NOT NULL
- **Business Rule**: Client activated their VVD card
- **Used By**: VDT (primary)

### 3. card_usage
- **Source**: HIVE (Spark parquet)
- **Table**: `/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT=`
- **Date Field**: TXN_DT (Transaction Date)
- **Client Key**: Extracted via SUBSTR(CLNT_CRD_NO, 7, 9) with leading zeros stripped via REGEXP_REPLACE("^0+", "")
- **Filters**:
  - SRVC_CD = 36
  - TXN_TYPES: [(TXN_TP=10, MSG_TP='0210'), (TXN_TP=13, MSG_TP='0210'), (TXN_TP=12, MSG_TP='0220')]
  - AMT1 > 0 (Positive transaction)
- **Business Rule**: Client used VVD card for POS transaction (specific types only)
- **Used By**: VUI (primary), VUT (secondary), VAW (secondary)

### 4. wallet_provisioning
- **Source**: EDW (Teradata cursor — NOT in Hive)
- **Tables**: DDWV05.CLNT_CRD_POS_LOG (B) + DL_DECMAN.TOKEN_LIST (C)
- **Date Field**: TXN_DT
- **Client Key**: SUBSTR(B.CLNT_CRD_NO, 7, 9) AS CLNT_NO (cast to integer to strip leading zeros)
- **Join**: B.TOKN_REQSTR_ID = C.TOKEN_ID
- **Filters**:
  - B.AMT1 = 0
  - SUBSTR(B.CLNT_CRD_NO, 1, 5) = '45190'
  - SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'
  - SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'
  - B.POS_ENTR_MODE_CD_NON_EMV = '000'
  - B.SRVC_CD = 36
  - C.TOKEN_WALLET_IND = 'Y'
- **Business Rule**: Client provisioned card to digital wallet (zero-amount with wallet token)
- **Used By**: VUT (primary), VAW (primary)

## CLNT_NO Format Agreement
All modules MUST use: String with NO leading zeros (stripped via REGEXP_REPLACE(..., "^0+", ""))
Enforced in M1 (tactic_evnt_hist), M3 (success outcomes), M5 (engagement).
**Critical**: Silent data loss occurs if any module produces CLNT_NO with leading zeros — joins fail silently.
