# VUT SAS Code — Full Transcription

Source: SAS Enterprise Guide — "WORKING_VUT_S..." (WORKING_VUT_Success or similar)
Photos: PXL_20260309_005734282.jpg through PXL_20260309_005803400.jpg (5 photos)

---

## Complete SAS Code

```sas
%include '/users/qjk9wbt/Password.txt';
option obs=max compress=yes reuse=yes mprint symbolgen;
options dlcreatedir;

%LET ME_DT = '2024-07-31';

%LET TACTIC = '2024232VUT';

/* ============================================================ */
/* STEP 1: Extract tactic population from tactic event history   */
/* ============================================================ */

proc sql;
  connect to teradata (mode=teradata user=&user password=&password);
  create table TACTIC as
  select * from connection to teradata (

    SELECT DISTINCT
        CLNT_NO/*client number */
        ,tactic_id
        ,tst_grp_cd
        ,rpt_grp_cd
        ,TREATMT_MN

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        where tactic_id = &tactic.
        order by 1
    );
quit;

proc freq data=TACTIC;
  tables
  tst_grp_cd
  / list missing;
  run;

/* ============================================================ */
/* STEP 2: Success01 — Wallet provisioning via merchant name     */
/* ============================================================ */

proc sql;
  connect to teradata (mode=teradata user=&user password=&password);
  create table Success01 as
  select * from connection to teradata (
    SELECT DISTINCT

        A.CLNT_NO
        ,A.TACTIC_ID
        ,A.TST_GRP_CD
        ,A.rpt_grp_cd
        ,A.TREATMT_MN
        ,min(txn_dt) as min_dt
        /*  ,max(txn_dt) as max_dt*/
        ,1 AS SUCCESS_IND

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

        INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B
        ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7, 9)

        INNER JOIN DL_DECMAN.TOKEN_LIST  as c
            on b.TOKN_REQSTR_ID  = c.TOKEN_ID

    WHERE B.TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT
        and       B.AMT1                              = 0
        and       SUBSTR(B.CLNT_CRD_NO,1, 5)         = '45190'
        and       SUBSTR(B.VISA_DR_CRD_NO,1, 5)      = '45199'
        and       SUBSTR(B.TOKN_REQSTR_ID,1,1)        > '0'
        and       B.POS_ENTR_MODE_CD_NON_EMV          = '000'
        and       B.SRVC_CD                            = 36
    AND       C.TOKEN_WALLET_IND = 'Y'

        and b.mrchnt_nm = 'Visa Provisioning Serv'
        and b.approval_code is not null

        AND A.tactic_id = &tactic.

    group by 1,2,3,4,5
    ORDER BY 1,2
    );
quit;

proc freq data=Success01;
  tables
  min_dt
  /*max_dt*/
  tst_grp_cd

  tst_grp_cd*min_dt

  / list missing;
  run;

/* ============================================================ */
/* STEP 3: Success02 — Wallet provisioning via tokn_vvd_pan      */
/* ============================================================ */

proc sql;
  connect to teradata (mode=teradata user=&user password=&password);
  create table Success02 as
  select * from connection to teradata (
    SELECT DISTINCT

        A.CLNT_NO
        ,A.TACTIC_ID
        ,A.TST_GRP_CD
        ,A.rpt_grp_cd
        ,A.TREATMT_MN
        ,min(txn_dt) as min_dt
        /*  ,max(txn_dt) as max_dt*/
        ,1 AS SUCCESS_IND

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

        INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B
        ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7, 9)

        INNER JOIN DL_DECMAN.TOKEN_LIST  as c
            on b.TOKN_REQSTR_ID  = c.TOKEN_ID

    WHERE B.TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT
        and       B.AMT1                              = 0
        and       SUBSTR(B.CLNT_CRD_NO,1, 5)         = '45190'
        and       SUBSTR(B.VISA_DR_CRD_NO,1, 5)      = '45199'
        and       SUBSTR(B.TOKN_REQSTR_ID,1,1)        > '0'
        and       B.POS_ENTR_MODE_CD_NON_EMV          = '000'
        and       B.SRVC_CD                            = 36
    AND       C.TOKEN_WALLET_IND = 'Y'

        and tokn_vvd_pan is not null

        AND A.tactic_id = &tactic.

    group by 1,2,3,4,5
    ORDER BY 1,2
    );
quit;

proc freq data=Success02;
  tables
  min_dt
  /*max_dt*/
  tst_grp_cd

  tst_grp_cd*min_dt

  / list missing;
  run;

/* ============================================================ */
/* STEP 4: Success03 — Wallet provisioning (variant 3)           */
/* ============================================================ */

proc sql;
  connect to teradata (mode=teradata user=&user password=&password);
  create table Success03 as
  select * from connection to teradata (
    SELECT DISTINCT

        A.CLNT_NO
        ,A.TACTIC_ID
        ,A.TST_GRP_CD
        ,A.rpt_grp_cd
        ,A.TREATMT_MN
        ,min(txn_dt) as min_dt
        /*  ,max(txn_dt) as max_dt*/
        ,1 AS SUCCESS_IND

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST AS A

        INNER JOIN DDWV05.CLNT_CRD_POS_LOG AS B
        ON A.CLNT_NO = SUBSTR(B.CLNT_CRD_NO,7, 9)

        INNER JOIN DL_DECMAN.TOKEN_LIST  as c
            on b.TOKN_REQSTR_ID  = c.TOKEN_ID

    WHERE B.TXN_DT BETWEEN A.TREATMT_STRT_DT-30 AND A.TREATMT_END_DT
        and       B.AMT1                              = 0
        and       SUBSTR(B.CLNT_CRD_NO,1, 5)         = '45190'
        and       SUBSTR(B.VISA_DR_CRD_NO,1, 5)      = '45199'
        and       SUBSTR(B.TOKN_REQSTR_ID,1,1)        > '0'
        and       B.POS_ENTR_MODE_CD_NON_EMV          = '000'
        and       B.SRVC_CD                            = 36
    AND       C.TOKEN_WALLET_IND = 'Y'

    /*    and tokn_vvd_pan is not null */

        AND A.tactic_id = &tactic.

    group by 1,2,3,4,5
    ORDER BY 1,2
    );
quit;

proc freq data=Success03;
  tables
  min_dt
  /*max_dt*/
  tst_grp_cd

  tst_grp_cd*min_dt

  / list missing;
  run;

/* ============================================================ */
/* STEP 5: Combine success definitions into tactic_flags          */
/* ============================================================ */

proc sql;

  create table tactic_flags as

  select a.*
    ,COALESCE(b.SUCCESS_IND,0) as PROV_merch
    ,COALESCE(c.SUCCESS_IND,0) as tokn_vvd
    ,COALESCE(c.SUCCESS_IND,0) as any_wallet

  from work.tactic as a

  left join work.success01 as b
    on a.clnt_no = b.clnt_no

  left join work.success02 as c
    on a.clnt_no = c.clnt_no

  left join work.success03 as d
    on a.clnt_no = d.clnt_no
  ;
quit;

proc freq data=tactic_flags;
  tables

  /*max_dt*/
  tst_grp_cd

  PROV_merch*tokn_vvd

  any_wallet

  / list missing;
  run;
```

---

## Code Structure Summary

| Step | Table | Description | Success Definition |
|------|-------|-------------|-------------------|
| 1 | TACTIC | Extract experiment population from `DG6V01.TACTIC_EVNT_IP_AR_HIST` for tactic `2024232VUT` | N/A — population only |
| 2 | Success01 | Wallet provisioning via `mrchnt_nm = 'Visa Provisioning Serv'` AND `approval_code is not null` | PROV_merch |
| 3 | Success02 | Wallet provisioning via `tokn_vvd_pan is not null` | tokn_vvd |
| 4 | Success03 | Wallet provisioning — broadest definition (no merchant name or tokn_vvd_pan filter) | any_wallet |
| 5 | tactic_flags | Left join all 3 success tables to tactic population | Combined flags |

## Key Technical Details

1. **Tactic ID**: `2024232VUT` (macro variable `&tactic`)
2. **Measurement end date**: `2024-07-31` (macro variable `ME_DT` — but not used in the queries shown)
3. **Treatment window**: `TREATMT_STRT_DT - 30` to `TREATMT_END_DT` (30 days BEFORE treatment start through treatment end)
4. **Join chain**: TACTIC_EVNT_IP_AR_HIST -> CLNT_CRD_POS_LOG (on CLNT_NO = SUBSTR(CLNT_CRD_NO,7,9)) -> DL_DECMAN.TOKEN_LIST (on TOKN_REQSTR_ID = TOKEN_ID)
5. **Card filters**: BIN prefix 45190 (CLNT_CRD_NO) and 45199 (VISA_DR_CRD_NO) — VVD card identifiers
6. **POS filters**: AMT1=0, POS_ENTR_MODE_CD_NON_EMV='000', SRVC_CD=36, TOKEN_WALLET_IND='Y'
7. **Three success variants**:
   - Success01: Strictest — requires `mrchnt_nm = 'Visa Provisioning Serv'` AND `approval_code is not null`
   - Success02: Medium — requires `tokn_vvd_pan is not null` (no merchant name check)
   - Success03: Broadest — no merchant name or tokn_vvd_pan check (just the base POS/token criteria)
8. **Bug in Step 5**: `any_wallet` uses `c.SUCCESS_IND` (Success02) instead of `d.SUCCESS_IND` (Success03). Should be `COALESCE(d.SUCCESS_IND,0) as any_wallet`.

## Notable Observations

- The `-30` in `TREATMT_STRT_DT-30` means success is counted if it happens up to 30 days BEFORE treatment start. This is unusual — may capture pre-existing provisioning behavior.
- `ME_DT` macro variable is defined but not referenced in any of the visible queries.
- The `group by 1,2,3,4,5` with `min(txn_dt)` means only the earliest transaction date per client is kept.
- `max(txn_dt)` is commented out in all three success queries.
- This is a SAS-based predecessor to the PySpark VVD v2 pipeline — same logic, different technology stack.
