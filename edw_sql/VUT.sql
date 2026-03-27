-- VUT — Tokenization | Metric: wallet_provisioning
-- Source tables: DG6V01.TACTIC_EVNT_IP_AR_HIST, DDWV05.CLNT_CRD_POS_LOG, DL_DECMAN.TOKEN_LIST
-- Success: zero-dollar provisioning txn (AMT1=0), VVD BIN (45190/45199), token confirmed
-- Note: add TXN_DT date filter to avoid full EDW scan (known performance gotcha)
-- Output: vintage curve (mnc, cohort, test, vintage, leads, success)

WITH tactic_history AS (
    SELECT
        TRIM(REGEXP_REPLACE(TACTIC_EVNT_ID, '^0+', ''))  AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT,
        TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')               AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

wallet_success AS (
    SELECT
        TRIM(LEADING '0' FROM CAST(SUBSTR(b.CLNT_CRD_NO, 7, 9) AS VARCHAR(9))) AS clnt_no,
        b.TXN_DT AS success_dt
    FROM DDWV05.CLNT_CRD_POS_LOG b
    JOIN DL_DECMAN.TOKEN_LIST t
        ON b.TOKN_REQSTR_ID = t.TOKEN_ID
    WHERE b.SRVC_CD = 36
      AND b.AMT1 = 0
      AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
      AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
      AND b.APPROVAL_CODE IS NOT NULL
      AND t.TOKEN_WALLET_IND = 'Y'
      AND b.TXN_DT >= DATE '2025-01-01'
),

denominator AS (
    SELECT
        cohort,
        test,
        COUNT(DISTINCT clnt_no) AS leads
    FROM tactic_history
    GROUP BY cohort, test
),

vintage_raw AS (
    SELECT
        a.test,
        a.cohort,
        (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN wallet_success b
        ON a.clnt_no = b.clnt_no
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.clnt_no, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
)

SELECT
    mnc,
    cohort,
    test,
    vintage,
    leads,
    SUM(day_success) OVER (
        PARTITION BY cohort, test
        ORDER BY vintage
        ROWS UNBOUNDED PRECEDING
    ) AS success
FROM (
    SELECT
        'VUT'              AS mnc,
        d.cohort,
        d.test,
        v.vintage,
        d.leads,
        COUNT(*)           AS day_success
    FROM denominator d
    JOIN vintage_raw v
        ON d.cohort = v.cohort
        AND d.test = v.test
    GROUP BY
        d.cohort,
        d.test,
        v.vintage,
        d.leads
) daily
ORDER BY
    cohort,
    test,
    vintage
;
