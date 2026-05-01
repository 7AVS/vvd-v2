-- =============================================================================
-- VVD VINTAGE CURVES — FINAL
-- Purpose:    Cumulative-success vintage curves for all 6 VVD campaigns.
--             Numbers will reconcile to VVD_SUCCESS_LOGIC_FINAL.sql.
--
-- Output schema (identical across all blocks):
--   MNE           CHAR     Campaign code: VCN/VDA/VDT/VUI/VUT/VAW
--   COHORT        CHAR     TO_CHAR(TREATMT_STRT_DT, 'YYYY-MM')
--   TST_GRP_CD    CHAR     TRIM(TST_GRP_CD) — test group
--   RPT_GRP_CD    CHAR     TRIM(RPT_GRP_CD) or 'OVERALL' for rollup
--   METRIC        CHAR     'PRIMARY' or 'SECONDARY'
--   VINTAGE_DAY   INT      0..29 (30d campaigns) or 0..89 (90d campaigns)
--   LEADS         INT      Distinct CLNT_NO denominator at grouping level
--   SUCCESS_CUM   INT      Cumulative distinct successes on or before VINTAGE_DAY
--   RATE          FLOAT    SUCCESS_CUM / NULLIFZERO(LEADS)
--
-- Campaigns and windows:
--   VCN  30d  primary only     acquisition: VISA_DR_CRD_DLY ISS_DT
--   VDA  90d  primary+secondary acquisition: VISA_DR_CRD_DLY ISS_DT / purchase: PT_OF_SALE_TXN
--   VDT  30d  primary only     activation:  VISA_DR_CRD_DLY ACTV_DT
--   VUI  30d  primary only     usage:       PT_OF_SALE_TXN TXN_DT
--   VUT  90d  primary+secondary provisioning: CLNT_CRD_POS_LOG / purchase: PT_OF_SALE_TXN
--   VAW  30d  primary+secondary provisioning: CLNT_CRD_POS_LOG / purchase: PT_OF_SALE_TXN
--
-- OVERALL rollup:  GROUPING SETS ((cohort,test,rpt_grp_cd,vintage),(cohort,test,vintage))
--                  Denominator counted at each grouping level via separate
--                  denominator CTEs (one per-channel, one overall) to avoid
--                  double-counting clients who appear in multiple RPT_GRP_CDs.
--
-- Source of truth: VVD_SUCCESS_LOGIC_FINAL.sql (locked 2026-05-01)
-- Tactic table:    DG6V01.TACTIC_EVNT_IP_AR_HIST
-- Date range:      TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
-- =============================================================================


-- =============================================================================
-- VCN — Acquisition campaign, 30d window, PRIMARY only
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),

vcn_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vcn_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(card.ISS_DT) AS first_iss_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.VISA_DR_CRD_DLY card
        ON a.CLNT_NO = card.CLNT_NO
        AND card.STS_CD IN ('06', '08')
        AND card.SRVC_ID = 36
        AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
        AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VCN'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vcn_first_success AS (
    SELECT
        t.cohort,
        t.tst_grp_cd,
        t.rpt_grp_cd,
        t.CLNT_NO,
        (s.first_iss_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vcn_tactic t
    INNER JOIN vcn_prim_success s
        ON t.CLNT_NO = s.CLNT_NO
        AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT
        ORDER BY s.first_iss_dt ASC
    ) = 1
),

-- Denominator: per-channel distinct clients
vcn_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd,
           COUNT(DISTINCT CLNT_NO) AS leads
    FROM vcn_tactic
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),

-- Denominator: overall distinct clients (not a sum of channels)
vcn_denom_overall AS (
    SELECT cohort, tst_grp_cd,
           COUNT(DISTINCT CLNT_NO) AS leads
    FROM vcn_tactic
    GROUP BY cohort, tst_grp_cd
),

-- Day-level success counts: per-channel
vcn_spd_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day,
           COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vcn_first_success
    WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),

-- Day-level success counts: overall
vcn_spd_overall AS (
    SELECT cohort, tst_grp_cd, vintage_day,
           COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vcn_first_success
    WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, vintage_day
),

-- Grid x channel
vcn_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vcn_denom_channel d
    CROSS JOIN day_seq s
),

-- Grid x overall
vcn_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vcn_denom_overall d
    CROSS JOIN day_seq s
)

SELECT
    'VCN'                                                                                  AS MNE,
    g.cohort                                                                               AS COHORT,
    g.tst_grp_cd                                                                           AS TST_GRP_CD,
    g.rpt_grp_cd                                                                           AS RPT_GRP_CD,
    'PRIMARY'                                                                              AS METRIC,
    g.vintage_day                                                                          AS VINTAGE_DAY,
    g.leads                                                                                AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day
        ROWS UNBOUNDED PRECEDING
    )                                                                                      AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day
        ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads)                                                         AS RATE
FROM vcn_grid_channel g
LEFT JOIN vcn_spd_channel spd
    ON g.cohort = spd.cohort
    AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd
    AND g.vintage_day = spd.vintage_day

UNION ALL

SELECT
    'VCN'                                                                                  AS MNE,
    g.cohort                                                                               AS COHORT,
    g.tst_grp_cd                                                                           AS TST_GRP_CD,
    'OVERALL'                                                                              AS RPT_GRP_CD,
    'PRIMARY'                                                                              AS METRIC,
    g.vintage_day                                                                          AS VINTAGE_DAY,
    g.leads                                                                                AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day
        ROWS UNBOUNDED PRECEDING
    )                                                                                      AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day
        ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads)                                                         AS RATE
FROM vcn_grid_overall g
LEFT JOIN vcn_spd_overall spd
    ON g.cohort = spd.cohort
    AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;


-- =============================================================================
-- VDA — Acquisition campaign, 90d window, PRIMARY (acquisition) + SECONDARY (purchase)
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 89
),

vda_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vda_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(card.ISS_DT) AS first_iss_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.VISA_DR_CRD_DLY card
        ON a.CLNT_NO = card.CLNT_NO
        AND card.STS_CD IN ('06', '08')
        AND card.SRVC_ID = 36
        AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
        AND card.ISS_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vda_sec_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(c.TXN_DT) AS first_txn_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.MSG_TP = '0220'
        AND c.AMT1 > 0
        AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
            )
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDA'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vda_first_prim AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_iss_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vda_tactic t
    INNER JOIN vda_prim_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_iss_dt ASC
    ) = 1
),

vda_first_sec AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_txn_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vda_tactic t
    INNER JOIN vda_sec_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_txn_dt ASC
    ) = 1
),

vda_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vda_tactic GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),
vda_denom_overall AS (
    SELECT cohort, tst_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vda_tactic GROUP BY cohort, tst_grp_cd
),

vda_spd_prim_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vda_first_prim WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vda_spd_prim_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vda_first_prim WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, vintage_day
),
vda_spd_sec_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vda_first_sec WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vda_spd_sec_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vda_first_sec WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, vintage_day
),

vda_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vda_denom_channel d CROSS JOIN day_seq s
),
vda_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vda_denom_overall d CROSS JOIN day_seq s
)

-- VDA PRIMARY per-channel
SELECT
    'VDA' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vda_grid_channel g
LEFT JOIN vda_spd_prim_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VDA PRIMARY overall
SELECT
    'VDA' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vda_grid_overall g
LEFT JOIN vda_spd_prim_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

UNION ALL

-- VDA SECONDARY per-channel
SELECT
    'VDA' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vda_grid_channel g
LEFT JOIN vda_spd_sec_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VDA SECONDARY overall
SELECT
    'VDA' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vda_grid_overall g
LEFT JOIN vda_spd_sec_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY METRIC, COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;


-- =============================================================================
-- VDT — Activation campaign, 30d window, PRIMARY only
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),

vdt_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vdt_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(card.ACTV_DT) AS first_actv_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.VISA_DR_CRD_DLY card
        ON a.CLNT_NO = card.CLNT_NO
        AND card.STS_CD IN ('06', '08')
        AND card.SRVC_ID = 36
        AND card.ACTV_DT IS NOT NULL
        AND card.SNAP_DT = (SELECT MAX(SNAP_DT) FROM DDWV01.VISA_DR_CRD_DLY)
        AND card.ACTV_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VDT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vdt_first_success AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_actv_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vdt_tactic t
    INNER JOIN vdt_prim_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_actv_dt ASC
    ) = 1
),

vdt_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vdt_tactic GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),
vdt_denom_overall AS (
    SELECT cohort, tst_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vdt_tactic GROUP BY cohort, tst_grp_cd
),

vdt_spd_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vdt_first_success WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vdt_spd_overall AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vdt_first_success WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, vintage_day
),

vdt_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vdt_denom_channel d CROSS JOIN day_seq s
),
vdt_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vdt_denom_overall d CROSS JOIN day_seq s
)

SELECT
    'VDT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vdt_grid_channel g
LEFT JOIN vdt_spd_channel spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

SELECT
    'VDT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vdt_grid_overall g
LEFT JOIN vdt_spd_overall spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;


-- =============================================================================
-- VUI — Usage campaign, 30d window, PRIMARY only (no secondary in FINAL.sql)
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),

vui_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vui_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(c.TXN_DT) AS first_txn_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.MSG_TP = '0220'
        AND c.AMT1 > 0
        AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
            )
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUI'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vui_first_success AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_txn_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vui_tactic t
    INNER JOIN vui_prim_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_txn_dt ASC
    ) = 1
),

vui_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vui_tactic GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),
vui_denom_overall AS (
    SELECT cohort, tst_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vui_tactic GROUP BY cohort, tst_grp_cd
),

vui_spd_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vui_first_success WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vui_spd_overall AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vui_first_success WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, vintage_day
),

vui_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vui_denom_channel d CROSS JOIN day_seq s
),
vui_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vui_denom_overall d CROSS JOIN day_seq s
)

SELECT
    'VUI' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vui_grid_channel g
LEFT JOIN vui_spd_channel spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

SELECT
    'VUI' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vui_grid_overall g
LEFT JOIN vui_spd_overall spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;


-- =============================================================================
-- VUT — Tokenization campaign, 90d window, PRIMARY (provisioning) + SECONDARY (purchase)
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 89
),

vut_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vut_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT) AS first_prov_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
        ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
        AND b.SRVC_CD = 36
        AND b.AMT1 = 0
        AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
        AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
        AND b.APPROVAL_CODE IS NOT NULL
        AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    INNER JOIN DL_DECMAN.TOKEN_LIST tk
        ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
        AND tk.TOKEN_WALLET_IND = 'Y'
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vut_sec_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(c.TXN_DT) AS first_txn_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.MSG_TP = '0220'
        AND c.AMT1 > 0
        AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
            )
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 89)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VUT'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vut_first_prim AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_prov_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vut_tactic t
    INNER JOIN vut_prim_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_prov_dt ASC
    ) = 1
),

vut_first_sec AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_txn_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vut_tactic t
    INNER JOIN vut_sec_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_txn_dt ASC
    ) = 1
),

vut_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vut_tactic GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),
vut_denom_overall AS (
    SELECT cohort, tst_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vut_tactic GROUP BY cohort, tst_grp_cd
),

vut_spd_prim_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vut_first_prim WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vut_spd_prim_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vut_first_prim WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, vintage_day
),
vut_spd_sec_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vut_first_sec WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vut_spd_sec_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vut_first_sec WHERE vintage_day BETWEEN 0 AND 89
    GROUP BY cohort, tst_grp_cd, vintage_day
),

vut_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vut_denom_channel d CROSS JOIN day_seq s
),
vut_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vut_denom_overall d CROSS JOIN day_seq s
)

-- VUT PRIMARY per-channel
SELECT
    'VUT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vut_grid_channel g
LEFT JOIN vut_spd_prim_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VUT PRIMARY overall
SELECT
    'VUT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vut_grid_overall g
LEFT JOIN vut_spd_prim_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

UNION ALL

-- VUT SECONDARY per-channel
SELECT
    'VUT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vut_grid_channel g
LEFT JOIN vut_spd_sec_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VUT SECONDARY overall
SELECT
    'VUT' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vut_grid_overall g
LEFT JOIN vut_spd_sec_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY METRIC, COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;


-- =============================================================================
-- VAW — Add To Wallet campaign, 30d window, PRIMARY (provisioning) + SECONDARY (purchase)
-- =============================================================================
WITH RECURSIVE day_seq (day_num) AS (
    SELECT 0 FROM sys_calendar.calendar WHERE calendar_date = DATE '1900-01-01'
    UNION ALL
    SELECT day_num + 1 FROM day_seq WHERE day_num < 29
),

vaw_tactic AS (
    SELECT
        a.CLNT_NO,
        TRIM(a.TST_GRP_CD)                             AS tst_grp_cd,
        TRIM(a.RPT_GRP_CD)                             AS rpt_grp_cd,
        a.TREATMT_STRT_DT,
        TO_CHAR(a.TREATMT_STRT_DT, 'YYYY-MM')          AS cohort
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
),

vaw_prim_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(b.TXN_DT) AS first_prov_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV05.CLNT_CRD_POS_LOG b
        ON a.CLNT_NO = SUBSTR(b.CLNT_CRD_NO, 7, 9)
        AND b.SRVC_CD = 36
        AND b.AMT1 = 0
        AND (b.VISA_DR_CRD_NO LIKE '45190%' OR b.VISA_DR_CRD_NO LIKE '45199%')
        AND b.POS_ENTR_MODE_CD_NON_EMV = '000'
        AND b.APPROVAL_CODE IS NOT NULL
        AND b.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
    INNER JOIN DL_DECMAN.TOKEN_LIST tk
        ON b.TOKN_REQSTR_ID = tk.TOKEN_ID
        AND tk.TOKEN_WALLET_IND = 'Y'
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vaw_sec_success AS (
    SELECT
        a.CLNT_NO,
        a.TREATMT_STRT_DT,
        MIN(c.TXN_DT) AS first_txn_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST a
    INNER JOIN DDWV01.PT_OF_SALE_TXN c
        ON a.CLNT_NO = SUBSTR(c.CLNT_CRD_NO, 7, 9)
        AND c.SRVC_CD = 36
        AND c.MSG_TP = '0220'
        AND c.AMT1 > 0
        AND (
                 (c.TXN_TP IN ('10','13') AND c.RCNCL_REAS_CD IN ('M','S'))
              OR (c.TXN_TP = '22'         AND c.RCNCL_REAS_CD IN ('P','E'))
            )
        AND c.TXN_DT BETWEEN a.TREATMT_STRT_DT AND (a.TREATMT_STRT_DT + 29)
    WHERE substr(a.TACTIC_ID, 8, 3) = 'VAW'
      AND a.TREATMT_STRT_DT BETWEEN DATE '2025-01-01' AND DATE '2026-03-31'
    GROUP BY a.CLNT_NO, a.TREATMT_STRT_DT
),

vaw_first_prim AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_prov_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vaw_tactic t
    INNER JOIN vaw_prim_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_prov_dt ASC
    ) = 1
),

vaw_first_sec AS (
    SELECT
        t.cohort, t.tst_grp_cd, t.rpt_grp_cd, t.CLNT_NO,
        (s.first_txn_dt - t.TREATMT_STRT_DT) AS vintage_day
    FROM vaw_tactic t
    INNER JOIN vaw_sec_success s
        ON t.CLNT_NO = s.CLNT_NO AND t.TREATMT_STRT_DT = s.TREATMT_STRT_DT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.CLNT_NO, t.TREATMT_STRT_DT ORDER BY s.first_txn_dt ASC
    ) = 1
),

vaw_denom_channel AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vaw_tactic GROUP BY cohort, tst_grp_cd, rpt_grp_cd
),
vaw_denom_overall AS (
    SELECT cohort, tst_grp_cd, COUNT(DISTINCT CLNT_NO) AS leads
    FROM vaw_tactic GROUP BY cohort, tst_grp_cd
),

vaw_spd_prim_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vaw_first_prim WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vaw_spd_prim_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vaw_first_prim WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, vintage_day
),
vaw_spd_sec_ch AS (
    SELECT cohort, tst_grp_cd, rpt_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vaw_first_sec WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, rpt_grp_cd, vintage_day
),
vaw_spd_sec_ov AS (
    SELECT cohort, tst_grp_cd, vintage_day, COUNT(DISTINCT CLNT_NO) AS day_success
    FROM vaw_first_sec WHERE vintage_day BETWEEN 0 AND 29
    GROUP BY cohort, tst_grp_cd, vintage_day
),

vaw_grid_channel AS (
    SELECT d.cohort, d.tst_grp_cd, d.rpt_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vaw_denom_channel d CROSS JOIN day_seq s
),
vaw_grid_overall AS (
    SELECT d.cohort, d.tst_grp_cd, d.leads, s.day_num AS vintage_day
    FROM vaw_denom_overall d CROSS JOIN day_seq s
)

-- VAW PRIMARY per-channel
SELECT
    'VAW' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vaw_grid_channel g
LEFT JOIN vaw_spd_prim_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VAW PRIMARY overall
SELECT
    'VAW' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'PRIMARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vaw_grid_overall g
LEFT JOIN vaw_spd_prim_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

UNION ALL

-- VAW SECONDARY per-channel
SELECT
    'VAW' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    g.rpt_grp_cd AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd, g.rpt_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vaw_grid_channel g
LEFT JOIN vaw_spd_sec_ch spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.rpt_grp_cd = spd.rpt_grp_cd AND g.vintage_day = spd.vintage_day

UNION ALL

-- VAW SECONDARY overall
SELECT
    'VAW' AS MNE, g.cohort AS COHORT, g.tst_grp_cd AS TST_GRP_CD,
    'OVERALL' AS RPT_GRP_CD, 'SECONDARY' AS METRIC,
    g.vintage_day AS VINTAGE_DAY, g.leads AS LEADS,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) AS SUCCESS_CUM,
    SUM(COALESCE(spd.day_success, 0)) OVER (
        PARTITION BY g.cohort, g.tst_grp_cd
        ORDER BY g.vintage_day ROWS UNBOUNDED PRECEDING
    ) * 1.0 / NULLIFZERO(g.leads) AS RATE
FROM vaw_grid_overall g
LEFT JOIN vaw_spd_sec_ov spd
    ON g.cohort = spd.cohort AND g.tst_grp_cd = spd.tst_grp_cd
    AND g.vintage_day = spd.vintage_day

ORDER BY METRIC, COHORT, TST_GRP_CD, RPT_GRP_CD, VINTAGE_DAY
;
