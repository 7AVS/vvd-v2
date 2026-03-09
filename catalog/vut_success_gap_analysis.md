# VUT/VAW Wallet Provisioning — Success Definition Gap Analysis

## Date: 2026-03-08

## Source: Old SAS code (photos from analyst's workstation)

## Old Logic: Three Success Variants Tested

The old SAS code (`WORKING_VUT_S...`) ran three parallel success definitions against `DDWV05.CLNT_CRD_POS_LOG` joined with `DL_DECMAN.TOKEN_LIST`:

### Common Filters (all three variants)
- `B.AMT1 = 0` (zero-dollar provisioning transaction)
- `SUBSTR(B.CLNT_CRD_NO, 1, 5) = '45190'` (VVD BIN)
- `SUBSTR(B.VISA_DR_CRD_NO, 1, 5) = '45199'` (VVD BIN)
- `SUBSTR(B.TOKN_REQSTR_ID, 1, 1) > '0'` (valid token requestor)
- `B.POS_ENTR_MODE_CD_NON_EMV = '000'`
- `B.SRVC_CD = 36`
- `C.TOKEN_WALLET_IND = 'Y'`
- Treatment window: `B.TXN_DT BETWEEN A.TREATMT_STRT_DT - 30 AND A.TREATMT_END_DT`

### Success01 (Strict — merchant + approval)
Additional filters:
- `B.MRCHNT_NM = 'Visa Provisioning Serv'`
- `B.APPROVAL_CODE IS NOT NULL`

### Success02 (Medium — token PAN)
Additional filter:
- `TOKN_VVD_PAN IS NOT NULL`

### Success03 (Broad — no additional filters)
No extra filters beyond the common set.

### Assembly
Left join all three to tactic table → flags: `PROV_merch`, `tokn_vvd`, `any_wallet`
Note: Bug in old code — `any_wallet` assigned from Success02 instead of Success03 (copy-paste error).

## v2 Pipeline Logic
- Used Success03 equivalent (broadest — no mrchnt_nm, no approval_code, no tokn_vvd_pan)
- Treatment window: `TREATMT_STRT_DT` to `TREATMT_END_DT` (no -30 day lookback)

## Gaps Identified

| Aspect | Old (SAS) | v2 Pipeline | Impact |
|---|---|---|---|
| Treatment window | STRT_DT - 30 to END_DT | STRT_DT to END_DT | Old captures 30 days of pre-treatment organic behavior |
| Merchant name filter | Success01: `MRCHNT_NM = 'Visa Provisioning Serv'` | Not present | v2 may include non-provisioning $0 transactions |
| Approval code filter | Success01: `APPROVAL_CODE IS NOT NULL` | Not present | v2 may include declined/failed provisioning attempts |
| Token PAN filter | Success02: `TOKN_VVD_PAN IS NOT NULL` | Not present | v2 may include incomplete provisioning |

## v3 Decision

**Adopted Success01 filters** with our treatment window:
- Added `MRCHNT_NM = 'Visa Provisioning Serv'` — confirms the $0 transaction is a Visa tokenization verification, not another type of $0 auth
- Added `APPROVAL_CODE IS NOT NULL` — confirms the issuer approved the provisioning (card is valid, active, not blocked)
- **Kept v2 treatment window** (no -30 day lookback) — the lookback contaminates the causal signal with pre-campaign organic behavior

**Applies to both VUT and VAW** — they share the same wallet_provisioning query (M3c).

## Filter Meanings (Business Terms)

- **`Visa Provisioning Serv`**: Visa's own system identity in POS logs. When a client adds a card to Apple Pay/Google Pay, Visa runs a $0 verification auth that appears under this merchant name. Isolates real provisioning from other $0 transactions.
- **`APPROVAL_CODE IS NOT NULL`**: The issuer's "yes, proceed" signal. Null = declined (card blocked, closed, fraud flagged). Ensures we only count successful provisioning.
- **`TOKN_VVD_PAN IS NOT NULL`**: The device token (DPAN) was created and registered. Proof of end-to-end completion. Not used in v3 (Success01 is sufficient).
- **`-30 day lookback`**: Legacy attribution buffer. Captures pre-treatment organic behavior. Removed in v2/v3 for cleaner causal measurement.
