# Campaign RPT_GRP_CD Configuration (from mega_output pivot tables)
Source: Photos PXL_20260308_200346340 through PXL_20260308_200528366

## Summary

| Campaign | RPT_GRP_CDs | # Groups | Cohorts | Missing Months | Has Email | Metrics |
|----------|-------------|----------|---------|----------------|-----------|---------|
| VAW | PVAWAG01 | 1 | 10 (2025-04 to 2026-02) | 2025-05 | No | card_usage, wallet_provisioning |
| VCN | PVCNAG01, PVCNAG02 | 2 | 12 (2025-02 to 2026-02) | 2025-10 | No | card_acquisition |
| VDA | PVDAAG03, PVDAAG04 | 2 | 2 (2025-07, 2025-11) | N/A (seasonal) | Yes | card_acquisition + 4 email |
| VDT | (not in these photos) | — | 14 (2025-01 to 2026-02) | — | — | card_activation |
| VUI | PVUIAG01, PVUIAG02, PVUIAG03, PVUIAG04 | 4 | 13 monthly (2025-02 to 2026-02) | — | Yes | card_usage + 4 email |
| VUT | PVUTAG01 | 1 | 1 (2025-06) | N/A (single deployment) | Yes | card_usage, wallet_provisioning + 4 email |

## Key Observations

1. **VCN has 2 report groups** (AG01, AG02) — likely channel split (e.g., MB vs IM). Both measure card_acquisition only. No email channel.
2. **VDA has 2 report groups** (AG03, AG04) — likely EM+IM vs IM-only or similar channel split. Both cohorts have email metrics.
3. **VUI is the most complex** — 4 report groups across 13 monthly cohorts. PVUIAG01=EM+IM, PVUIAG02=EM only (from earlier memory). All have email metrics.
4. **VAW has NO email metrics** — pure in-app/MB channel. Single report group.
5. **VUT has email but only 1 cohort** — single deployment June 2025, single report group.
6. **Control (TG7) has email metrics too** — seen in VDA, VUI, VUT. This means Control IS communicated to via email (or at minimum the email fields are tracked for them).
7. **Missing months**: VAW skips 2025-05, VCN skips 2025-10. Could be deployment gaps or data issues.

## VDA Detail
- PVDAAG03: Both cohorts (2025-07, 2025-11) × both test groups × 5 metrics = 20 rows
- PVDAAG04: Same structure = 20 rows
- Total: 40 rows

## VUI Detail (per cohort)
- 4 RPT_GRP_CDs × 2 test groups × 5 metrics = 40 rows per cohort
- 13 cohorts × 40 = 520 rows total (most data of any campaign)

## VUT Detail
- 1 RPT_GRP_CD × 2 test groups × 6 metrics = 12 rows total (least data)
