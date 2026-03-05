# VVD v2 Pipeline: Source Tables

## Hive/Spark Parquet (on HDFS)

| Table | Path | Key Fields | Used By |
|-------|------|-----------|---------|
| Experiment Metadata (tactic_evnt_hist) | `/prod/sz/tsz/00150/cc/DTZTA_T_TACTIC_EVNT_HIST/EVNT_STRT_DT={year}*` | TACTIC_EVNT_ID (→CLNT_NO), TACTIC_ID, TREATMT_STRT_DT, TREATMT_END_DT, TST_GRP_CD, RPT_GRP_CD | M1 |
| Card Data (DDWTA_VISA_DR_CRD) | `/prod/sz/tsz/00050/data/DDWTA_VISA_DR_CRD/PartitionColumn=Latest/CAPTR_DT={year}*` | CLNT_NO, ISS_DT, ACTV_DT, STS_CD, SRVC_ID | M3 (card_acquisition, card_activation) |
| POS Transactions | `/prod/sz/tsz/00050/data/DDWTA_T_PT_OF_SALE_TXN/SNAP_DT={year}*` | CLNT_CRD_NO (→CLNT_NO), TXN_DT, TXN_TP, MSG_TP, AMT1, SRVC_CD | M3 (card_usage) |

## EDW/Teradata Tables (via EDW.cursor())

| Table | Location | Key Fields | Used By |
|-------|----------|-----------|---------|
| CLNT_CRD_POS_LOG | DDWV05.CLNT_CRD_POS_LOG | CLNT_CRD_NO, VISA_DR_CRD_NO, TOKN_REQSTR_ID, TXN_DT, AMT1, POS_ENTR_MODE_CD_NON_EMV, SRVC_CD | M3 (wallet_provisioning) |
| TOKEN_LIST | DL_DECMAN.TOKEN_LIST | TOKEN_ID, TOKEN_WALLET_IND | M3 (wallet_provisioning) |
| Vendor Feedback Master | DTZV01.VENDOR_FEEDBACK_MASTER | Email feedback tracking | M5 |
| Vendor Feedback Event | DTZV01.VENDOR_FEEDBACK_EVENT | Email engagement events | M5 |
| Fulfillment History | DG6V01.TACTIC_EVNT_IP_AR_HIST | Tactic fulfillment records | M5 |

## Not In Pipeline (External Dependencies)

| Table | Description | Status |
|-------|-------------|--------|
| UCP (Unified Client Profile) | ~100 demographic fields (AGE, INCOME, TENURE, etc.) | No extraction pipeline exists — must be sourced from data engineering |
| final_df.parquet | Pre-built analytical table with 199 fields | VVD v1 artifact — will be replaced by pipeline output |

## Join Keys

All tables join on CLNT_NO (string, no leading zeros). The standard extraction pattern is:
```
REGEXP_REPLACE(SUBSTR(CLNT_CRD_NO, 7, 9), "^0+", "")  -- for card/txn tables
REGEXP_REPLACE(TRIM(TACTIC_EVNT_ID), "^0+", "")         -- for tactic_evnt_hist
```
