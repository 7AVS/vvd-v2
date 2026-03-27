-- VUI — Usage Trigger | Metric: card_usage (purchase transaction)
-- Source tables: DG6V01.TACTIC_EVNT_IP_AR_HIST, DDWV05.CLNT_CRD_POS_LOG
-- TODO: Confirm POS table name — may be DDWV01.PT_OF_SALE_TXN or similar (txn_tp column presence varies)
-- CLNT_NO derivation: strip leading zeros from positions 7-9 of CLNT_CRD_NO
-- Output: vintage curve (mnc, treatmt_end_dt, test, vintage, leads, success)

WITH tactic_history AS (
    SELECT
        TRIM(REGEXP_REPLACE(TACTIC_EVNT_ID, '^0+', ''))  AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND substr(a.TACTIC_DECISN_VRB_INFO, 1, 10) = a.TACTIC_ID
      AND a.Tactic_adnc_typ_cd = 1
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_END_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

txn_success AS (
    SELECT
        TRIM(LEADING '0' FROM SUBSTR(b.CLNT_CRD_NO, 7, 9)) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    WHERE b.SRVC_CD = 36
      AND b.AMT1 > 0
      AND b.txn_tp IN (10, 13)
      AND b.TXN_DT BETWEEN DATE '2025-01-01' AND DATE '2026-04-30'
),

denominator AS (
    SELECT
        TREATMT_END_DT,
        test,
        COUNT(DISTINCT clnt_no) AS leads
    FROM tactic_history
    GROUP BY TREATMT_END_DT, test
),

vintage_raw AS (
    SELECT
        a.test,
        a.TREATMT_END_DT,
        (b.success_dt - a.TREATMT_END_DT) AS vintage
    FROM tactic_history a
    JOIN txn_success b
        ON a.clnt_no = b.clnt_no
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.clnt_no, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
)

SELECT
    'VUI'              AS mnc,
    d.TREATMT_END_DT,
    d.test,
    v.vintage,
    d.leads,
    COUNT(*)           AS success
FROM denominator d
JOIN vintage_raw v
    ON d.TREATMT_END_DT = v.TREATMT_END_DT
    AND d.test = v.test
GROUP BY
    d.TREATMT_END_DT,
    d.test,
    v.vintage,
    d.leads
ORDER BY
    d.TREATMT_END_DT,
    d.test,
    v.vintage
;
