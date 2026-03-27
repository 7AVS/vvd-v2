-- VDA — Seasonal Acquisition | Metric: card_acquisition
-- Source tables: DG6V01.TACTIC_EVNT_IP_AR_HIST, DDWV01.VISA_DR_CRD_DIY
-- Output: vintage curve (mnc, treatmt_end_dt, test, vintage, leads, success)

WITH tactic_history AS (
    SELECT
        TRIM(REGEXP_REPLACE(TACTIC_EVNT_ID, '^0+', ''))  AS clnt_no,
        TRIM(TST_GRP_CD)                                  AS test,
        TREATMT_STRT_DT,
        TREATMT_END_DT
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND TRIM(TST_GRP_CD) IN ('TG4', 'TG7')
      AND a.TREATMT_END_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

card_success AS (
    SELECT
        TRIM(REGEXP_REPLACE(CAST(CLNT_NO AS VARCHAR(20)), '^0+', '')) AS clnt_no,
        ISS_DT AS success_dt
    FROM DDWV01.VISA_DR_CRD_DIY
    WHERE STS_CD IN ('06', '08')
      AND SRVC_ID = 36
      AND SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DIY)
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
        (b.success_dt - a.TREATMT_STRT_DT) AS vintage
    FROM tactic_history a
    JOIN card_success b
        ON a.clnt_no = b.clnt_no
        AND b.success_dt BETWEEN a.TREATMT_STRT_DT AND a.TREATMT_END_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.clnt_no, a.TREATMT_STRT_DT
        ORDER BY b.success_dt ASC
    ) = 1
)

SELECT
    'VDA'              AS mnc,
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
