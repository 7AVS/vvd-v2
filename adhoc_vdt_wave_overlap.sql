-- VDT Wave Overlap: Are AG11/AG13 clients a subset of AG01/AG03?
-- Run in Hue against tactic_evnt_hist

WITH vdt_clients AS (
    SELECT DISTINCT
        regexp_replace(trim(TACTIC_EVNT_ID), '^0+', '') AS clnt_no,
        trim(RPT_GRP_CD) AS rpt_grp_cd
    FROM tactic_evnt_hist
    WHERE substring(TACTIC_ID, 8, 3) = 'VDT'
      AND trim(TST_GRP_CD) = 'TG4'
      AND trim(RPT_GRP_CD) IN ('PVDTAG01','PVDTAG02','PVDTAG03','PVDTAG04','PVDTAG11','PVDTAG13')
),
ag01 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG01'),
ag02 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG02'),
ag03 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG03'),
ag11 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG11'),
ag13 AS (SELECT clnt_no FROM vdt_clients WHERE rpt_grp_cd = 'PVDTAG13')

SELECT
    'AG11 total' AS check_name,
    COUNT(*) AS cnt
FROM ag11

UNION ALL

SELECT
    'AG11 in AG01 (expect ~100%)',
    COUNT(*)
FROM ag11 a INNER JOIN ag01 b ON a.clnt_no = b.clnt_no

UNION ALL

SELECT
    'AG11 in AG02 (DO crossover?)',
    COUNT(*)
FROM ag11 a INNER JOIN ag02 b ON a.clnt_no = b.clnt_no

UNION ALL

SELECT
    'AG13 total',
    COUNT(*)
FROM ag13

UNION ALL

SELECT
    'AG13 in AG03 (expect ~100%)',
    COUNT(*)
FROM ag13 a INNER JOIN ag03 b ON a.clnt_no = b.clnt_no
;
