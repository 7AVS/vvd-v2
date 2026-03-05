# VVD v2 Pipeline: Reconciliation — VVD v1 vs Vintage/Vvd

Field-by-field comparison between the original VVD analysis (v1, from final_df.parquet) and the Vintage/Vvd pipeline.

## Core Fields

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| Client ID | CLNT_NO | CLNT_NO | YES | Both strip leading zeros |
| Test Group | TST_GRP_CD (TG4/TG7) | TST_GRP_CD | YES | TG4=Test in both |
| Campaign ID | MNE | MNE | YES | Same 6 campaigns |
| Treatment Start | TREATMT_STRT_DT | TREATMT_STRT_DT | YES | — |
| Treatment End | TREATMT_END_DT | TREATMT_END_DT | YES | — |
| Report Group | RPT_GRP_CD | RPT_GRP_CD | YES | — |
| Tactic Segment | TACTIC_SEG | Not present | GAP | v1 only — used for sub-segment analysis |

## Success Metrics

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| VCN success | ACQUISITION_SUCCESS (binary) | card_acquisition (event-level) | PARTIAL | Different granularity — v1 is pre-aggregated binary, Vintage is event-level with SUCCESS_DT |
| VDA success | ACTIVATION_SUCCESS (workaround) | card_acquisition (ISS_DT) | RESOLVED | **Decision**: Use card_acquisition from Vintage/Vvd. Replaces backend workaround. |
| VDT success | ACTIVATION_SUCCESS | card_activation (ACTV_DT) | YES | Same underlying logic |
| VUI success | USAGE_SUCCESS | card_usage (TXN_DT) | YES | Same underlying logic |
| VUT success | PROVISIONING_SUCCESS | wallet_provisioning | YES | Same underlying logic |
| VAW success | PROVISIONING_SUCCESS | wallet_provisioning | RESOLVED | **Decision**: Use wallet_provisioning. Semantically correct for "Add To Wallet". |

## Measurement & Analysis

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| Measurement window | Not explicit in final_df (implicit in dates) | 30 or 90 days per campaign, configurable | GAP | Vintage/Vvd is explicit — adopt this approach |
| Success granularity | Binary flag per deployment row | Event-level: CLNT_NO + SUCCESS_DT | PARTIAL | Vintage provides richer data (days to success, count of events) |
| Vintage curves | Not present | Cumulative success by day, cohort, test group | GAP | New capability from Vintage/Vvd |
| Channel breakdown | CHANNEL field present | M5 engagement module (email sent/open/click/unsub) | PARTIAL | Vintage adds email engagement tracking |

## Demographics

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| Demographics | ~100 UCP fields pre-joined | NOT PRESENT | GAP | UCP pipeline needed separately — see UCP_GAP_ANALYSIS.md |
| UCP metadata | UCP_REFERENCE_DATE, UCP_MONTH_END_DATE | NOT PRESENT | GAP | Snapshot dating for demographic fields |

## Experiment Population

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| Population source | Pre-built in final_df (27M rows) | M1 from tactic_evnt_hist | PARTIAL | Vintage/Vvd has the extraction logic; final_df was pre-built |
| Cohort definition | Not explicit | COHORT = yyyy-MM of TREATMT_STRT_DT | GAP | Vintage approach is cleaner |
| Statistical testing | Chi-square SRM, z-test proportions | CONFIDENCE_LEVEL = 0.95 | PARTIAL | v1 has more tests; Vintage is simpler |

## Output Shape

| Concept | VVD v1 (final_df) | Vintage/Vvd | Match? | Resolution |
|---------|-------------------|-------------|--------|------------|
| Output format | Flat table: 1 row per client-tactic-deployment | vintage_curves: 1 row per MNE-COHORT-TST_GRP-METRIC-DAY | DIFFERENT | Different analytical purposes — v1 is for ad-hoc analysis, Vintage is for curve tracking |
| Output fields | 199 fields (demographics + outcomes) | ~10 fields (MNE, COHORT, TST_GRP_CD, RPT_GRP_CD, METRIC, DAY, WINDOW_DAYS, CLIENT_CNT, SUCCESS_CNT, RATE) | DIFFERENT | Vintage is aggregated; v1 is client-level |

## Summary of Resolutions

All MISMATCH items have been resolved:
- VDA: Use card_acquisition (ISS_DT based) — replaces ACTIVATION_SUCCESS workaround
- VAW: Use wallet_provisioning — semantically correct for "Add To Wallet"
- Measurement windows: Adopt Vintage/Vvd's explicit per-campaign configuration
- Demographics: External dependency — documented in UCP_GAP_ANALYSIS.md
