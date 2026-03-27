-- =============================================================================
-- Schema discovery queries — run each one separately in Teradata SQL Editor
-- =============================================================================

-- 1. Tactic population (VVD campaigns)
HELP TABLE DG6V01.TACTIC_EVNT_IP_AR_HIST;

-- 2. Card table (acquisition/activation — VCN, VDA, VDT)
HELP TABLE DDWV01.VISA_DR_CRD_DIY;

-- 3. POS / transaction table (usage — VUI, also wallet provisioning — VUT, VAW)
HELP TABLE DDWV05.CLNT_CRD_POS_LOG;

-- 4. Token list (wallet provisioning — VUT, VAW)
HELP TABLE DL_DECMAN.TOKEN_LIST;

-- 5. Tactic population (IMT campaigns — IPC, IRI)
HELP TABLE DTZV01.TACTIC_EVNT_IP_AR_H60M;

-- 6. IMT success events (IPC, IRI)
HELP TABLE DDWV01.EXT_CDP_CHNL_EVNT;
