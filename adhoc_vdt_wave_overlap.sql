-- VDT Wave Overlap: Are AG11/AG13 clients a subset of AG01/AG03?
-- Limited to 2026-02 cohort to avoid spool overflow

WITH vdt_clients AS (
    SELECT DISTINCT
        regexp_replace(trim(TACTIC_EVNT_ID), '^0+', '') AS clnt_no,
        trim(RPT_GRP_CD) AS rpt_grp_cd
    FROM tactic_evnt_hist
    WHERE trim(RPT_GRP_CD) IN ('PVDTAG01','PVDTAG02','PVDTAG03','PVDTAG11','PVDTAG13')
      AND trim(TST_GRP_CD) = 'TG4'
      AND DCSN_DT >= '2026-02-01' AND DCSN_DT < '2026-03-01'
),
ag01 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG01'),
ag02 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG02'),
ag03 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG03'),
ag11 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG11'),
ag13 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG13')

SELECT 'AG11 total' AS check_name, COUNT(*) AS cnt FROM ag11
UNION ALL
SELECT 'AG11 in AG01', COUNT(*) FROM ag11 a JOIN ag01 b ON a.clnt_no = b.clnt_no
UNION ALL
SELECT 'AG11 in AG02 (DO?)', COUNT(*) FROM ag11 a JOIN ag02 b ON a.clnt_no = b.clnt_no
UNION ALL
SELECT 'AG13 total', COUNT(*) FROM ag13
UNION ALL
SELECT 'AG13 in AG03', COUNT(*) FROM ag13 a JOIN ag03 b ON a.clnt_no = b.clnt_no
;
